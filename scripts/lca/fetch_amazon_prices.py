# scripts/lca/fetch_amazon_prices.py
"""
Fetch provisional Amazon UK replacement-price estimates and calculate
item-level embodied CO2 values.

This script replaces the earlier hard-coded Excel workflow:

    - old workflow:
        prices_to_excel.py had hard-coded item lists and wrote price results
        to local Excel files.

    - new workflow:
        this script pulls canonical item names directly from the SQLite
        database, retrieves Amazon UK price samples, calculates the agreed
        provisional spend-based embodied CO2 estimate, and writes the results
        back into the database table embodied_carbon_data.

Run from the project root, for example:

    python -m scripts.lca.fetch_amazon_prices --profile tom --db test_db --limit 5

Then, once tested:

    python -m scripts.lca.fetch_amazon_prices --profile tom --db inventory_db

Required database inputs:
    item_dictionary.item_name
    item_dictionary.ons_price
    item_dictionary.furniture_class
    furniture.emission_factor_CO2

Required output table:
    embodied_carbon_data

Current calculation method:
    1. Scrape up to 10 Amazon UK prices for each item.
    2. Calculate:
           amazon_price_mean = mean(top Amazon prices)
           amazon_price_std  = standard deviation(top Amazon prices)
    3. Calculate:
           amazon_price_upper = amazon_price_mean + amazon_price_std

       This follows the provisional method inferred from Sarka's workbook:
       using mean + 1 standard deviation as a more conservative Amazon-derived
       replacement cost estimate.

    4. Select replacement cost:
           if item_dictionary.ons_price is present:
               replacement_cost_adjusted = ons_price
           else:
               replacement_cost_adjusted = amazon_price_upper

       In other words, ons_price is treated as a curated override.

    5. Calculate embodied CO2:
           embodied_CO2_kg = replacement_cost_adjusted
                              * emission_factor_CO2
                              * 0.5

       The 0.5 factor represents the current project assumption that, on
       average, the fire brings forward replacement halfway through the
       product lifespan. The full replacement emissions are therefore halved
       to estimate the additional fire-attributable embodied CO2.

Important limitations:
    - Amazon scraping is brittle and should be treated as provisional.
    - Search-result prices may not represent exact like-for-like replacement.
    - The item_name values are used as search terms for now.
    - Future improvement: add a curated amazon_search_term column if some
      canonical item names produce poor search results.
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

from scripts.path_config import load_local_paths_config


# ---------------------------------------------------------------------------
# Constants controlling the provisional embodied-carbon calculation
# ---------------------------------------------------------------------------

# Current project assumption:
#
#   The item would eventually have been replaced anyway. Because we do not
#   know where in its service life the fire occurred, we assume that, on
#   average, the fire brings replacement forward by half of the product
#   lifespan.
#
#   Therefore:
#
#       fire-attributable embodied CO2 = full replacement CO2 * 0.5
#
FIRE_ATTRIBUTABLE_REPLACEMENT_FRACTION = 0.5


# Default user-agent copied from the earlier Amazon scraping script.
# This can be overridden from the command line with --user-agent.
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Small data containers
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ItemPricingInput:
    """
    Input data required to calculate embodied CO2 for one item.

    item_name:
        Canonical item name from item_dictionary. This is also used as the
        Amazon search term in this first database-driven version.

    ons_price:
        Optional curated/ONS replacement price. If present, this overrides
        the Amazon-derived price estimate.

    furniture_class:
        Category assigned to the item in item_dictionary.

    emission_factor_CO2:
        Furniture-class-level spend-based emissions factor, read from
        furniture.emission_factor_CO2.

        Expected unit:
            kg CO2 per £
        or equivalent project-specific spend-based factor.
    """

    item_name: str
    item_description: str
    price_search_term: str | None
    ons_price: float | None
    furniture_class: str
    emission_factor_CO2: float


@dataclass(frozen=True)
class PriceResult:
    """
    Calculated Amazon price summary for one item.

    prices:
        Up to 10 individual prices found in the Amazon UK search results.

    amazon_price_mean:
        Mean of the prices found. None if no prices were found.

    amazon_price_std:
        Population standard deviation of the prices found. This matches the
        default behaviour of numpy.std used in the earlier script.
        None if no prices were found.

    amazon_price_upper:
        Current provisional adjusted Amazon price:
            amazon_price_mean + amazon_price_std
        None if no prices were found.
    """

    prices: list[float]
    amazon_price_mean: float | None
    amazon_price_std: float | None
    amazon_price_upper: float | None


# ---------------------------------------------------------------------------
# Config / path helpers
# ---------------------------------------------------------------------------

def resolve_db_path(profile: str, db_handle: str, config: dict) -> Path:
    """
    Resolve the SQLite database path from config/local_paths.yaml.

    This is a local database-only resolver for the LCA helper. It mirrors the
    model dispatcher behaviour: this script needs the database path, but it
    does not need a raw input directory.
    """
    profiles = config.get("profiles", {})
    db_roots = config.get("db_roots", {})

    if profile not in profiles:
        raise KeyError(
            f"Profile '{profile}' not found in config.\n"
            f"Available profiles: {', '.join(sorted(profiles.keys())) or '(none)'}"
        )

    if db_handle not in db_roots:
        raise KeyError(
            f"DB handle '{db_handle}' not found in config.\n"
            f"Available DB handles: {', '.join(sorted(db_roots.keys())) or '(none)'}"
        )

    sharepoint_root = Path(profiles[profile]["sharepoint_root"])

    db_cfg = db_roots[db_handle]

    root = db_cfg.get("root")
    if not root:
        raise KeyError(f"Missing required db_roots.{db_handle}.root in config.")

    rel_db = db_cfg.get("rel_db")
    if not rel_db:
        raise KeyError(f"Missing required db_roots.{db_handle}.rel_db in config.")

    return sharepoint_root / Path(root) / Path(rel_db)


# ---------------------------------------------------------------------------
# Database validation and item retrieval
# ---------------------------------------------------------------------------

def get_table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    """
    Return the column names present in a SQLite table.

    Used for fail-fast validation so that this script gives a clear error if
    the database has not been reinitialised after the schema changes.
    """
    rows = conn.execute(f"PRAGMA table_info({table_name});").fetchall()

    if not rows:
        raise ValueError(
            f"Required table '{table_name}' was not found in the database."
        )

    return {row[1] for row in rows}


def require_columns(
    conn: sqlite3.Connection,
    table_name: str,
    required_columns: Iterable[str],
) -> None:
    """
    Check that a table contains all required columns.

    This avoids harder-to-understand SQL errors later in the workflow.
    """
    found_columns = get_table_columns(conn, table_name)
    missing = sorted(set(required_columns) - found_columns)

    if missing:
        raise ValueError(
            f"Table '{table_name}' is missing required column(s): {missing}.\n"
            f"Found columns: {sorted(found_columns)}"
        )


def validate_schema(conn: sqlite3.Connection) -> None:
    """
    Validate that the database contains the columns required by this script.

    The schema itself should be created by scripts.inventory.init_inventory_db.
    This function only checks that the current database is compatible.
    """
    require_columns(
        conn,
        "item_dictionary",
        ["item_name", "price_search_term", "ons_price", "furniture_class"],
    )

    require_columns(
        conn,
        "furniture",
        ["furniture_class", "emission_factor_CO2"],
    )

    require_columns(
        conn,
        "embodied_carbon_data",
        [
            "item_name",
            "amazon_price_top_1",
            "amazon_price_top_2",
            "amazon_price_top_3",
            "amazon_price_top_4",
            "amazon_price_top_5",
            "amazon_price_top_6",
            "amazon_price_top_7",
            "amazon_price_top_8",
            "amazon_price_top_9",
            "amazon_price_top_10",
            "amazon_price_mean",
            "amazon_price_std",
            "amazon_price_upper",
            "replacement_cost_adjusted",
            "embodied_CO2_kg",
            "notes",
        ],
    )


def validate_lca_inputs(conn: sqlite3.Connection) -> None:
    """
    Fail fast if any item cannot be connected to a valid emissions factor.

    The embodied CO2 calculation needs:

        item_dictionary.item_name
            -> item_dictionary.furniture_class
            -> furniture.emission_factor_CO2

    This validation makes sure every item has:
        - a furniture_class;
        - a matching furniture row;
        - a positive emission_factor_CO2.
    """
    bad_rows = conn.execute(
        """
        SELECT
            i.item_name,
            i.furniture_class,
            f.emission_factor_CO2
        FROM item_dictionary AS i
        LEFT JOIN furniture AS f
            ON i.furniture_class = f.furniture_class
        WHERE
            i.furniture_class IS NULL
            OR f.furniture_class IS NULL
            OR f.emission_factor_CO2 IS NULL
            OR f.emission_factor_CO2 <= 0
        ORDER BY i.item_name;
        """
    ).fetchall()

    if bad_rows:
        preview = "\n".join(
            f"  item_name={row[0]!r}, furniture_class={row[1]!r}, "
            f"emission_factor_CO2={row[2]!r}"
            for row in bad_rows[:20]
        )

        raise ValueError(
            "Some item_dictionary rows cannot be linked to a valid positive "
            "furniture.emission_factor_CO2 value.\n"
            "Fix mapping_list.xlsx and rerun the vocab ingest.\n\n"
            f"First affected rows:\n{preview}"
        )


def fetch_item_pricing_inputs(
    conn: sqlite3.Connection,
    *,
    limit: int | None = None,
    only_missing: bool = False,
) -> list[ItemPricingInput]:
    """
    Fetch the canonical list of items to process.

    The item list comes from item_dictionary, not from hard-coded Python lists.

    Parameters
    ----------
    limit:
        Optional limit for testing. For example, --limit 5 processes only the
        first five items alphabetically.

    only_missing:
        If True, only process items that do not yet have an Amazon mean price
        in embodied_carbon_data.
    """
    where_clause = ""

    if only_missing:
        where_clause = """
        WHERE
            e.item_name IS NULL
            OR e.amazon_price_mean IS NULL
        """

    sql = f"""
        SELECT
            i.item_name,
            i.item_description,
            i.price_search_term,
            i.ons_price,
            i.furniture_class,
            f.emission_factor_CO2
        FROM item_dictionary AS i
        JOIN furniture AS f
            ON i.furniture_class = f.furniture_class
        LEFT JOIN embodied_carbon_data AS e
            ON i.item_name = e.item_name
        {where_clause}
        ORDER BY i.item_name
    """

    params: list[object] = []

    if limit is not None:
        sql += "\nLIMIT ?"
        params.append(limit)

    rows = conn.execute(sql, params).fetchall()

    return [
        ItemPricingInput(
            item_name=str(row[0]),
            item_description=str(row[1]).strip(),
            price_search_term=None if row[2] is None else str(row[2]).strip(),
            ons_price=None if row[3] is None else float(row[3]),
            furniture_class=str(row[4]),
            emission_factor_CO2=float(row[5]),
        )
        for row in rows
    ]


# ---------------------------------------------------------------------------
# Amazon scraping helpers
# ---------------------------------------------------------------------------

def build_amazon_search_url(item_name: str) -> str:
    """
    Build an Amazon UK search URL for one item.

    The earlier script used:
        item.replace(" ", "+")

    This version uses quote_plus(), which does the same thing for spaces but
    also handles characters such as slashes more safely.
    """
    return f"https://www.amazon.co.uk/s?k={quote_plus(item_name)}"


def parse_price_text(price_text: str) -> float | None:
    """
    Parse visible Amazon price text into a positive float.

    Examples this aims to handle:
        "£12.99"
        "12.99"
        "1,249.00"
        "£1,249"

    Returns None if no valid positive numeric value can be extracted.
    """
    if price_text is None:
        return None

    cleaned = (
        str(price_text)
        .replace("£", "")
        .replace(",", "")
        .replace("\n", "")
        .strip()
    )

    # Extract the first number-like value from the cleaned text.
    # This is intentionally conservative: if the text contains no number,
    # the function returns None.
    match = re.search(r"\d+(?:\.\d+)?", cleaned)

    if match is None:
        return None

    try:
        price = float(match.group(0))
    except ValueError:
        return None

    if price <= 0:
        return None

    return price


def extract_prices_from_html(html: str, *, max_prices: int = 10) -> list[float]:
    """
    Extract up to max_prices Amazon prices from search-result HTML.

    The original Sarka script extracted:
        span.a-price-whole

    This function keeps that fallback, but first tries to extract the full
    visible price from:
        span.a-price span.a-offscreen

    That is useful because a-price-whole may lose the pence component, while
    a-offscreen often contains the complete visible price, e.g. "£12.99".
    """
    soup = BeautifulSoup(html, "html.parser")

    prices: list[float] = []

    # Preferred route: complete visible price text, when available.
    for price_element in soup.select("span.a-price span.a-offscreen"):
        price = parse_price_text(price_element.get_text())

        if price is None:
            continue

        prices.append(price)

        if len(prices) >= max_prices:
            return prices

    # Fallback route: reproduce the older behaviour as closely as possible.
    # This may ignore pence, but it is better than failing if a-offscreen is
    # missing from the returned HTML.
    for price_element in soup.find_all("span", class_="a-price-whole"):
        price = parse_price_text(price_element.get_text())

        if price is None:
            continue

        prices.append(price)

        if len(prices) >= max_prices:
            return prices

    return prices


def fetch_amazon_prices_for_item(
    item_name: str,
    *,
    max_prices: int,
    timeout_s: int,
    user_agent: str,
) -> list[float]:
    """
    Fetch up to max_prices Amazon UK prices for one item.

    Raises an HTTP/network exception if the request fails. The main loop catches
    those exceptions per item so that one failed item does not stop the entire
    batch.
    """
    url = build_amazon_search_url(item_name)

    headers = {
        "user-agent": user_agent,
    }

    response = requests.get(url, headers=headers, timeout=timeout_s)
    response.raise_for_status()

    return extract_prices_from_html(response.text, max_prices=max_prices)


# ---------------------------------------------------------------------------
# Price and embodied-carbon calculations
# ---------------------------------------------------------------------------

def choose_price_search_term(item: ItemPricingInput) -> str:
    """
    Choose the text used for Amazon price searching.

    Preferred order:
        1. item_dictionary.price_search_term, if provided
        2. item_dictionary.item_description, as the default fallback

    item_name is deliberately not used here because it is an internal database
    key rather than a human/product search phrase.
    """
    if item.price_search_term is not None and item.price_search_term.strip():
        return item.price_search_term.strip()

    return item.item_description.strip()


def pad_prices(prices: Iterable[float], *, n: int = 10) -> list[float | None]:
    """
    Pad a list of prices to exactly n entries using None.

    This allows a variable number of scraped prices to be stored in fixed
    database columns:

        amazon_price_top_1 ... amazon_price_top_10
    """
    padded: list[float | None] = list(prices)[:n]

    while len(padded) < n:
        padded.append(None)

    return padded


def summarise_prices(prices: list[float]) -> PriceResult:
    """
    Calculate Amazon price summary values.

    amazon_price_mean:
        Mean of the scraped prices.

    amazon_price_std:
        Population standard deviation of the scraped prices.

        The earlier script used numpy.std(), which uses population standard
        deviation by default. statistics.pstdev() is the standard-library
        equivalent.

    amazon_price_upper:
        Current provisional adjusted Amazon price:
            amazon_price_mean + amazon_price_std

        This is treated as a more conservative replacement-cost proxy than the
        mean alone.
    """
    if not prices:
        return PriceResult(
            prices=[],
            amazon_price_mean=None,
            amazon_price_std=None,
            amazon_price_upper=None,
        )

    amazon_price_mean = float(statistics.mean(prices))

    if len(prices) == 1:
        amazon_price_std = 0.0
    else:
        amazon_price_std = float(statistics.pstdev(prices))

    amazon_price_upper = amazon_price_mean + amazon_price_std

    return PriceResult(
        prices=prices,
        amazon_price_mean=amazon_price_mean,
        amazon_price_std=amazon_price_std,
        amazon_price_upper=amazon_price_upper,
    )


def calculate_replacement_cost_adjusted(
    *,
    ons_price: float | None,
    amazon_price_upper: float | None,
) -> float | None:
    """
    Select the replacement cost used in the spend-based CO2 calculation.

    Current agreed logic:
        1. If ons_price is available, use it.
        2. Otherwise, use amazon_price_upper.

    Where:
        amazon_price_upper = amazon_price_mean + amazon_price_std

    This means a curated ONS/reference price overrides the scraped Amazon
    estimate. If neither value is available, the replacement cost is unknown
    and the function returns None.
    """
    if ons_price is not None:
        return ons_price

    return amazon_price_upper


def calculate_embodied_CO2_kg(
    *,
    replacement_cost_adjusted: float | None,
    emission_factor_CO2: float,
) -> float | None:
    """
    Calculate fire-attributable embodied CO2.

    Current agreed formula:
        embodied_CO2_kg =
            replacement_cost_adjusted
            * emission_factor_CO2
            * FIRE_ATTRIBUTABLE_REPLACEMENT_FRACTION

    Interpreted as:
        replacement_cost_adjusted:
            selected replacement cost in £

        emission_factor_CO2:
            spend-based emissions factor, currently stored at furniture-class
            level in furniture.emission_factor_CO2

        FIRE_ATTRIBUTABLE_REPLACEMENT_FRACTION:
            currently 0.5, representing the assumption that the fire causes
            replacement halfway through the product lifespan on average.

    If replacement_cost_adjusted is unknown, the embodied CO2 cannot be
    calculated and the function returns None.
    """
    if replacement_cost_adjusted is None:
        return None

    return (
        replacement_cost_adjusted
        * emission_factor_CO2
        * FIRE_ATTRIBUTABLE_REPLACEMENT_FRACTION
    )


# ---------------------------------------------------------------------------
# Database writing
# ---------------------------------------------------------------------------

def upsert_embodied_carbon_data(
    conn: sqlite3.Connection,
    *,
    item_name: str,
    price_result: PriceResult,
    replacement_cost_adjusted: float | None,
    embodied_CO2_kg: float | None,
    notes: str,
) -> None:
    """
    Insert or update one row in embodied_carbon_data.

    The table is keyed by item_name, with one current LCA/spend-based result
    per canonical item.

    This upsert updates the scraped Amazon evidence and the calculated
    embodied CO2 result while preserving the one-row-per-item structure.
    """
    padded_prices = pad_prices(price_result.prices, n=10)

    conn.execute(
        """
        INSERT INTO embodied_carbon_data (
            item_name,

            amazon_price_top_1,
            amazon_price_top_2,
            amazon_price_top_3,
            amazon_price_top_4,
            amazon_price_top_5,
            amazon_price_top_6,
            amazon_price_top_7,
            amazon_price_top_8,
            amazon_price_top_9,
            amazon_price_top_10,

            amazon_price_mean,
            amazon_price_std,
            amazon_price_upper,

            replacement_cost_adjusted,
            embodied_CO2_kg,

            notes
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(item_name) DO UPDATE SET
            amazon_price_top_1 = excluded.amazon_price_top_1,
            amazon_price_top_2 = excluded.amazon_price_top_2,
            amazon_price_top_3 = excluded.amazon_price_top_3,
            amazon_price_top_4 = excluded.amazon_price_top_4,
            amazon_price_top_5 = excluded.amazon_price_top_5,
            amazon_price_top_6 = excluded.amazon_price_top_6,
            amazon_price_top_7 = excluded.amazon_price_top_7,
            amazon_price_top_8 = excluded.amazon_price_top_8,
            amazon_price_top_9 = excluded.amazon_price_top_9,
            amazon_price_top_10 = excluded.amazon_price_top_10,

            amazon_price_mean = excluded.amazon_price_mean,
            amazon_price_std = excluded.amazon_price_std,
            amazon_price_upper = excluded.amazon_price_upper,

            replacement_cost_adjusted = excluded.replacement_cost_adjusted,
            embodied_CO2_kg = excluded.embodied_CO2_kg,

            notes = excluded.notes
        ;
        """,
        (
            item_name,

            padded_prices[0],
            padded_prices[1],
            padded_prices[2],
            padded_prices[3],
            padded_prices[4],
            padded_prices[5],
            padded_prices[6],
            padded_prices[7],
            padded_prices[8],
            padded_prices[9],

            price_result.amazon_price_mean,
            price_result.amazon_price_std,
            price_result.amazon_price_upper,

            replacement_cost_adjusted,
            embodied_CO2_kg,

            notes,
        ),
    )


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    """
    Command-line entry point.

    This function keeps the full workflow visible:

        config/local_paths.yaml
            -> resolved database path
            -> canonical item list from item_dictionary
            -> Amazon price retrieval
            -> replacement-cost calculation
            -> embodied CO2 calculation
            -> embodied_carbon_data upsert
    """
    parser = argparse.ArgumentParser(
        prog="fetch_amazon_prices",
        description=(
            "Fetch Amazon UK price samples for item_dictionary items and write "
            "provisional embodied CO2 values to embodied_carbon_data."
        ),
    )

    parser.add_argument(
        "--profile",
        required=True,
        help="Profile name from config/local_paths.yaml, e.g. tom.",
    )

    parser.add_argument(
        "--db",
        required=True,
        help="Database handle from config/local_paths.yaml, e.g. test_db or inventory_db.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help=(
            "Optional number of items to process. Useful for testing, e.g. "
            "--limit 5."
        ),
    )

    parser.add_argument(
        "--only-missing",
        action="store_true",
        help=(
            "Only process items with no existing amazon_price_mean in "
            "embodied_carbon_data."
        ),
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=2.0,
        help="Seconds to wait between Amazon requests. Default: 2.0.",
    )

    parser.add_argument(
        "--max-prices",
        type=int,
        default=10,
        help="Maximum number of Amazon prices to store per item. Default: 10.",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Request timeout in seconds. Default: 20.",
    )

    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="User-agent header for Amazon requests.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Fetch and calculate values, but do not write to the database. "
            "Useful for checking a small --limit run."
        ),
    )

    args = parser.parse_args(argv)

    if args.max_prices < 1 or args.max_prices > 10:
        print("\nERROR: --max-prices must be between 1 and 10.")
        return 2

    if args.limit is not None and args.limit <= 0:
        print("\nERROR: --limit must be a positive integer if provided.")
        return 2

    if args.sleep < 0:
        print("\nERROR: --sleep must be >= 0.")
        return 2

    # Resolve database path.
    config = load_local_paths_config(Path("config") / "local_paths.yaml")
    db_path = resolve_db_path(args.profile, args.db, config)

    print("Resolved paths:")
    print(f"  DB HANDLE: {args.db}")
    print(f"  DB:        {db_path}")

    if not db_path.exists():
        print("\nERROR: Database file does not exist at resolved path.")
        return 2

    rows_attempted = 0
    rows_written = 0
    rows_failed_fetch = 0
    rows_without_price_basis = 0
    rows_without_embodied_CO2 = 0

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

        # Fail early if the database is missing required schema changes.
        validate_schema(conn)

        # Fail early if item -> furniture_class -> emission_factor_CO2 lookup
        # is incomplete.
        validate_lca_inputs(conn)

        item_inputs = fetch_item_pricing_inputs(
            conn,
            limit=args.limit,
            only_missing=args.only_missing,
        )

        if not item_inputs:
            print("\nNo items to process.")
            return 0

        print(f"\nItems to process: {len(item_inputs)}")
        print(f"Dry run:          {args.dry_run}")

        for index, item in enumerate(item_inputs, start=1):
            rows_attempted += 1

            # Choose the text that will actually be sent to Amazon
            search_term = choose_price_search_term(item)

            print(f"\n[{index}/{len(item_inputs)}] {item.item_name}")
            print(f"  Description:           {item.item_description}")
            print(f"  Search term used:      {search_term}")
            print(f"  Furniture class:       {item.furniture_class}")
            print(f"  ONS/reference price:   {item.ons_price}")
            print(f"  emission_factor_CO2:   {item.emission_factor_CO2}")

            fetch_error: Exception | None = None

            try:
                prices = fetch_amazon_prices_for_item(
                    search_term,
                    max_prices=args.max_prices,
                    timeout_s=args.timeout,
                    user_agent=args.user_agent,
                )

                price_result = summarise_prices(prices)

            except Exception as e:
                # Do not stop the whole batch if one Amazon request fails.
                # If ons_price exists, the row can still receive an embodied
                # CO2 calculation using that curated replacement price.
                fetch_error = e
                rows_failed_fetch += 1

                print(f"  Amazon fetch ERROR: {e}")

                price_result = PriceResult(
                    prices=[],
                    amazon_price_mean=None,
                    amazon_price_std=None,
                    amazon_price_upper=None,
                )

            replacement_cost_adjusted = calculate_replacement_cost_adjusted(
                ons_price=item.ons_price,
                amazon_price_upper=price_result.amazon_price_upper,
            )

            embodied_CO2_kg = calculate_embodied_CO2_kg(
                replacement_cost_adjusted=replacement_cost_adjusted,
                emission_factor_CO2=item.emission_factor_CO2,
            )

            if replacement_cost_adjusted is None:
                rows_without_price_basis += 1

            if embodied_CO2_kg is None:
                rows_without_embodied_CO2 += 1

            if fetch_error is None:
                notes = (
                    f"Automated Amazon UK search-price sample using search term {search_term!r}. "
                    "Search term comes from item_dictionary.price_search_term if provided; "
                    "otherwise item_dictionary.item_description. "
                    "amazon_price_upper = amazon_price_mean + amazon_price_std. "
                    "replacement_cost_adjusted uses ons_price if available; "
                    "otherwise amazon_price_upper. "
                    "embodied_CO2_kg = replacement_cost_adjusted * "
                    "emission_factor_CO2 * 0.5."
                )
            else:
                notes = (
                    f"Amazon price fetch failed using search term {search_term!r}: {fetch_error}. "
                    "If ons_price was available, replacement_cost_adjusted and "
                    "embodied_CO2_kg were still calculated from ons_price. "
                    "Otherwise calculated values are NULL."
                )

            print(f"  Prices found:          {price_result.prices}")
            print(f"  Amazon mean:           {price_result.amazon_price_mean}")
            print(f"  Amazon std:            {price_result.amazon_price_std}")
            print(f"  Amazon upper:          {price_result.amazon_price_upper}")
            print(f"  Replacement cost used: {replacement_cost_adjusted}")
            print(f"  embodied_CO2_kg:       {embodied_CO2_kg}")

            if not args.dry_run:
                upsert_embodied_carbon_data(
                    conn,
                    item_name=item.item_name,
                    price_result=price_result,
                    replacement_cost_adjusted=replacement_cost_adjusted,
                    embodied_CO2_kg=embodied_CO2_kg,
                    notes=notes,
                )

                conn.commit()
                rows_written += 1

            if args.sleep > 0 and index < len(item_inputs):
                time.sleep(args.sleep)

    print("\nAmazon price / embodied CO2 workflow complete:")
    print(f"  Rows attempted:             {rows_attempted}")
    print(f"  Rows written:               {rows_written}")
    print(f"  Amazon fetch failures:      {rows_failed_fetch}")
    print(f"  Rows without price basis:   {rows_without_price_basis}")
    print(f"  Rows without embodied CO2:  {rows_without_embodied_CO2}")

    if args.dry_run:
        print("\nDry run only: no database rows were written.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())