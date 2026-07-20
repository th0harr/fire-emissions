# scripts/lca/fetch_amazon_prices.py
"""
Fetch provisional Amazon UK replacement-price estimates and calculate
item-level embodied CO2 values.

This script replaces the earlier hard-coded Excel workflow:

    - old workflow:
        prices_to_excel.py had hard-coded item lists and wrote price results
        to local Excel files.

    - new workflow:
        this script pulls canonical item records directly from the SQLite
        database, uses either a curated price_search_term or the item
        description as the Amazon UK search phrase, retrieves Amazon UK price
        samples, calculates the agreed provisional spend-based embodied CO2
        estimate, and writes the results back into the database table
        embodied_carbon_data.

Run from the project root, for example:

    python -m scripts.lca.fetch_amazon_prices --profile tom --db test_db --limit 5

Then, once tested:

    python -m scripts.lca.fetch_amazon_prices --profile tom --db inventory_db

Required database inputs:
    item_dictionary.item_name
    item_dictionary.item_description
    item_dictionary.price_search_term
    item_dictionary.ons_price
    item_dictionary.defra_spend_factor_CO2
    item_dictionary.furniture_class

Required output table:
    embodied_carbon_data

Current calculation method:
    1. Select the Amazon UK search term for each item:
           if item_dictionary.price_search_term is present:
               search_term = price_search_term
           else:
               search_term = item_description

       Note: price_search_term is recommended to avoid returning high sale-volume,
       low-price accessories sharing the same search terms as item_description.

    2. Scrape up to 10 Amazon UK prices for each selected search term.

       Because Amazon sometimes returns a valid HTTP response that contains no
       parseable product-price elements, this script retries each item until at
       least --min-prices values have been parsed, or until --max-retries
       attempts have been used. The default is intentionally conservative:

           --max-prices 10
           --min-prices 10
           --max-retries 10

       If the minimum number of prices cannot be retrieved, the script records
       a structured warning at the end of the run.

    3. Calculate:
           amazon_price_mean = mean(top Amazon prices)
           amazon_price_std  = standard deviation(top Amazon prices)

    4. Calculate:
           amazon_price_upper = amazon_price_mean + amazon_price_std

       This follows the provisional method from Sarka's workbook:
       using mean + 1 standard deviation as a more conservative Amazon-derived
       replacement cost estimate.

    5. Select replacement cost:
           if item_dictionary.ons_price is present:
               replacement_cost_adjusted = ons_price
           else:
               replacement_cost_adjusted = amazon_price_upper

       In other words, ons_price is treated as a curated price override.
       Note: ONS = Office of National Statistics.

    6. Calculate embodied CO2:
           embodied_CO2_kg = replacement_cost_adjusted
                              * defra_spend_factor_CO2
                              * 0.5

       Defra_spend_factor_CO2 values are taken from a DEFRA database.
       
       The 0.5 factor represents the current project assumption that, on
       average, the fire brings forward replacement halfway through the
       product lifespan. The full replacement emissions are therefore halved
       to estimate the additional fire-attributable embodied CO2.

Important limitations:
    - Amazon scraping is brittle and should be treated as provisional.
    - Search-result prices may not represent exact like-for-like replacement.
    - price_search_term values should be manually reviewed for items where
      item_description gives poor or overly broad Amazon search results.
    - defra_spend_factor_CO2 values should be checked/updated before final reporting.
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

from scripts.path_config import load_local_paths_config, resolve_db_path


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
        Canonical internal database key from item_dictionary. This is not used
        directly as the Amazon search term.

    item_description:
        Human-readable item description from item_dictionary. Used as the
        default Amazon search term when price_search_term is blank.

    price_search_term:
        Optional curated Amazon/search-engine phrase from item_dictionary. If
        provided, this is preferred over item_description.

    ons_price:
        Optional curated/ONS replacement price. If present, this overrides
        the Amazon-derived price estimate.


    defra_spend_factor_CO2:
        Item-level spend-based emissions factor, read from
        item_dictionary.defra_spend_factor_CO2.

        Expected unit:
            kg CO2 per £
        or equivalent project-specific spend-based factor.
    """

    item_name: str
    item_description: str
    price_search_term: str | None
    ons_price: float | None
    furniture_class: str
    defra_spend_factor_CO2: float


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


@dataclass(frozen=True)
class PriceFetchRetryResult:
    """
    Result from Amazon price fetching with retry / quality control.

    prices:
        The best parsed price list obtained across all attempts. If at least
        min_prices values were parsed on any attempt, this is that accepted
        attempt's price list. Otherwise, this is the longest partial list found.

    attempts_used:
        Number of fetch attempts used for this item.

    met_min_prices:
        True if one attempt parsed at least min_prices values. False means the
        script is using a partial or empty result and should record a warning.

    warning_type / message:
        Structured warning information for incomplete retrieval. Both are None
        when met_min_prices is True.
    """

    prices: list[float]
    attempts_used: int
    met_min_prices: bool
    warning_type: str | None
    message: str | None


@dataclass(frozen=True)
class PriceFetchWarning:
    """
    Structured warning for incomplete Amazon price retrieval.

    These warnings are collected during the run and printed together at the end,
    so the user can review potentially unreliable item rows without having to
    scroll through the full terminal output.
    """

    item_name: str
    raw_search_term: str
    cleaned_search_term: str
    prices_found: int
    min_prices_required: int
    attempts_used: int
    max_retries: int
    warning_type: str
    message: str



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
        [
            "item_name",
            "item_description",
            "price_search_term",
            "ons_price",
            "furniture_class",
            "defra_spend_factor_CO2",
        ],
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
    Fail fast if any item does not have a valid item-level DEFRA spend factor.

    The embodied CO2 calculation needs:

        item_dictionary.defra_spend_factor_CO2

    This is stored at item level because the DEFRA spend-based categories do
    not align cleanly with this project's furniture_class categories.
    """
    bad_rows = conn.execute(
        """
        SELECT
            item_name,
            defra_spend_factor_CO2
        FROM item_dictionary
        WHERE
            defra_spend_factor_CO2 IS NULL
            OR defra_spend_factor_CO2 <= 0
        ORDER BY item_name;
        """
    ).fetchall()

    if bad_rows:
        preview = "\n".join(
            f"  item_name={row[0]!r}, defra_spend_factor_CO2={row[1]!r}"
            for row in bad_rows[:20]
        )

        raise ValueError(
            "Some item_dictionary rows do not have a valid positive "
            "defra_spend_factor_CO2 value.\n"
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
            i.defra_spend_factor_CO2
        FROM item_dictionary AS i
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
            defra_spend_factor_CO2=float(row[5]),
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

    This intentionally uses only the 'a-price-whole' component, matching
    Sarka's original scraper.

    Note:
        Amazon prices are often represented in nested HTML, with whole-pound
        and pence components split across different elements.    
        Using only the 'a-price-whole' component risks occasionally missing
        the pense component from the price, however it avoids occasional 
        severe parsing artefacts which resulted in missing decimal points from prices.

        For this provisional spend-based estimate, losing the pence component
        is much less damaging than accidentally multiplying a price by orders
        of magnitude. More expensive items, which contribute most to embodied
        CO2, are also proportionally less affected by losing at most £0.99.



    Returns:
        A list of whole-pound price values as floats.
    """
    soup = BeautifulSoup(html, "html.parser")

    prices: list[float] = []

    # Match Sarka's original method: extract the visible whole-pound part
    # of each Amazon price.
    price_elements = soup.find_all("span", class_="a-price-whole")

    for price_element in price_elements[:max_prices]:
        price_text = price_element.get_text().strip()

        # Amazon whole-price strings may include commas and sometimes a
        # trailing full stop. For example:
        #   "1,249"  -> 1249
        #   "3."     -> 3
        cleaned = (
            price_text
            .replace(",", "")
            .replace(".", "")
            .strip()
        )

        if not cleaned:
            continue

        try:
            price = float(cleaned)
        except ValueError:
            continue

        if price <= 0:
            continue

        prices.append(price)

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

    Raises an HTTP/network exception if the request fails. The retry wrapper
    below catches those exceptions per attempt, so that intermittent Amazon
    response failures do not stop the entire batch.
    """
    url = build_amazon_search_url(item_name)

    headers = {
        "user-agent": user_agent,
    }

    response = requests.get(url, headers=headers, timeout=timeout_s)
    response.raise_for_status()

    return extract_prices_from_html(response.text, max_prices=max_prices)


def fetch_amazon_prices_with_retry(
    item_name: str,
    *,
    max_prices: int,
    min_prices: int,
    max_retries: int,
    sleep_s: float,
    timeout_s: int,
    user_agent: str,
) -> PriceFetchRetryResult:
    """
    Fetch Amazon prices, retrying when too few prices are parsed.

    Why this helper exists:
        During testing, Amazon sometimes returned a valid HTTP response that
        contained no parseable price elements. That can happen if Amazon returns
        a soft-block, cookie/location page, bot-check page, sponsored-layout
        variant, or other HTML that does not contain the expected
        span.a-price-whole elements.

    Quality-control rule:
        Accept an item result only once at least min_prices prices have been
        parsed from a single attempt. Otherwise, retry up to max_retries times.

    Important:
        The check is based on len(prices), not on the padded database columns.
        For example, if --max-prices 3 is used in a test run,
        amazon_price_top_4 ... amazon_price_top_10 are intentionally NULL.

    Returns:
        PriceFetchRetryResult containing either:
            - the accepted price list, if min_prices was reached; or
            - the longest partial price list found, with warning metadata.
    """
    best_prices: list[float] = []
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            prices = fetch_amazon_prices_for_item(
                item_name,
                max_prices=max_prices,
                timeout_s=timeout_s,
                user_agent=user_agent,
            )
        except Exception as e:
            # Keep trying after transient HTTP/network/parser-level failures.
            # The caller will receive a structured warning if all attempts fail
            # or if no attempt reaches min_prices.
            last_error = e
            print(
                f"  WARNING: attempt {attempt}/{max_retries} raised an error: {e}"
            )
        else:
            # Keep the best partial result in case all attempts fail to reach
            # the required minimum.
            if len(prices) > len(best_prices):
                best_prices = prices

            if len(prices) >= min_prices:
                if attempt > 1:
                    print(
                        f"  Accepted Amazon result after {attempt} attempt(s): "
                        f"{len(prices)} prices parsed."
                    )

                return PriceFetchRetryResult(
                    prices=prices,
                    attempts_used=attempt,
                    met_min_prices=True,
                    warning_type=None,
                    message=None,
                )

            print(
                f"  WARNING: attempt {attempt}/{max_retries} parsed "
                f"{len(prices)} price(s); required {min_prices}."
            )

        if attempt < max_retries:
            time.sleep(sleep_s)

    # If we reach this point, no attempt produced enough prices. Provide a
    # structured result rather than raising, so the batch can continue and the
    # user receives an end-of-process warning summary.
    if best_prices:
        warning_type = "amazon_partial_prices"
        message = (
            f"Parsed at most {len(best_prices)} price(s) after {max_retries} "
            f"attempt(s); required {min_prices}. Using the best partial result."
        )
    elif last_error is not None:
        warning_type = "amazon_fetch_failed"
        message = (
            f"No prices were parsed after {max_retries} attempt(s); last error: "
            f"{last_error}"
        )
    else:
        warning_type = "amazon_no_prices"
        message = (
            f"No prices were parsed after {max_retries} attempt(s); required "
            f"{min_prices}. Amazon may have returned non-product HTML."
        )

    return PriceFetchRetryResult(
        prices=best_prices,
        attempts_used=max_retries,
        met_min_prices=False,
        warning_type=warning_type,
        message=message,
    )


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


def clean_search_term_for_amazon(search_term: str) -> str:
    """
    Clean the selected search term before sending it to Amazon.

    The database value is left unchanged; this function only prepares the
    query string used in the Amazon URL.

    Rationale:
        item_description and price_search_term may contain punctuation used
        for human readability, e.g. "Drawers: large" or "CDs or DVDs (small shelves)".
        These characters are safe in a URL once encoded, but they are unlikely
        to improve Amazon search quality.

    Current cleaning:
        - replace punctuation/symbols with spaces;
        - keep letters, numbers, and whitespace;
        - collapse repeated whitespace;
        - preserve word order.
    """
    cleaned = re.sub(r"[^A-Za-z0-9\s]", " ", search_term)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    if not cleaned:
        raise ValueError(
            f"Search term became empty after cleaning: {search_term!r}"
        )

    return cleaned


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
    defra_spend_factor_CO2: float,
) -> float | None:
    """
    Calculate fire-attributable embodied CO2.

    Current agreed formula:
        embodied_CO2_kg =
            replacement_cost_adjusted
            * defra_spend_factor_CO2
            * FIRE_ATTRIBUTABLE_REPLACEMENT_FRACTION

    Interpreted as:
        replacement_cost_adjusted:
            selected replacement cost in £

        defra_spend_factor_CO2:
            spend-based emissions factor,
            currently stored at item-level in item_disctionary.defra_spend_factor_CO2

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
        * defra_spend_factor_CO2
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
        "--min-prices",
        type=int,
        default=10,
        help=(
            "Minimum number of parsed Amazon prices required before accepting "
            "an item as a complete Amazon sample. Default: 10."
        ),
    )

    parser.add_argument(
        "--max-retries",
        type=int,
        default=10,
        help=(
            "Maximum number of Amazon fetch attempts per item when fewer than "
            "--min-prices prices are parsed. Default: 10."
        ),
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

    if args.min_prices < 1:
        print("\nERROR: --min-prices must be a positive integer.")
        return 2

    if args.min_prices > args.max_prices:
        print("\nERROR: --min-prices cannot be greater than --max-prices.")
        print(
            "For example, if using --max-prices 3 for testing, also use "
            "--min-prices 3."
        )
        return 2

    if args.max_retries < 1:
        print("\nERROR: --max-retries must be a positive integer.")
        return 2

    if args.limit is not None and args.limit <= 0:
        print("\nERROR: --limit must be a positive integer if provided.")
        return 2

    if args.sleep < 0:
        print("\nERROR: --sleep must be >= 0.")
        return 2

    # Resolve database path.
    config = load_local_paths_config(Path("config") / "local_paths.yaml")
    resolved = resolve_db_path(args.profile, args.db, config)
    db_path = resolved.db_path

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
    rows_with_incomplete_amazon_sample = 0
    rows_skipped_due_to_price_fetch = 0

    # Collected warnings are printed at the end in a structured block. This is
    # easier to review than scanning the full item-by-item terminal output.
    price_fetch_warnings: list[PriceFetchWarning] = []

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

        # Fail early if the database is missing required schema changes.
        validate_schema(conn)

        # Fail early if any item is missing a valid DEFRA spend-based CO2 factor.
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
        print(
            "Amazon QC:       "
            f"max_prices={args.max_prices}, min_prices={args.min_prices}, "
            f"max_retries={args.max_retries}"
        )

        for index, item in enumerate(item_inputs, start=1):
            rows_attempted += 1

            # Choose the text that will actually be sent to Amazon
            raw_search_term = choose_price_search_term(item)
            search_term = clean_search_term_for_amazon(raw_search_term)

            print(f"\n[{index}/{len(item_inputs)}]  {item.item_name}")
            print(f"  Description:            {item.item_description}")
            print(f"  Raw search term:        {raw_search_term}")
            print(f"  Cleaned search term:    {search_term}")
            print(f"  Furniture class:        {item.furniture_class}")
            print(f"  ONS/reference price:    {item.ons_price}")
            print(f"  DEFRA spend factor CO2: {item.defra_spend_factor_CO2}")

            fetch_error: Exception | None = None
            retry_result: PriceFetchRetryResult | None = None

            try:
                retry_result = fetch_amazon_prices_with_retry(
                    search_term,
                    max_prices=args.max_prices,
                    min_prices=args.min_prices,
                    max_retries=args.max_retries,
                    # Retry pauses should be long enough to reduce the chance
                    # of repeated Amazon soft-block / empty-layout responses.
                    # If the user has set a larger --sleep value, respect it.
                    sleep_s=max(args.sleep, 5.0),
                    timeout_s=args.timeout,
                    user_agent=args.user_agent,
                )

                if not retry_result.met_min_prices:
                    rows_with_incomplete_amazon_sample += 1

                    warning = PriceFetchWarning(
                        item_name=item.item_name,
                        raw_search_term=raw_search_term,
                        cleaned_search_term=search_term,
                        prices_found=len(retry_result.prices),
                        min_prices_required=args.min_prices,
                        attempts_used=retry_result.attempts_used,
                        max_retries=args.max_retries,
                        warning_type=retry_result.warning_type or "amazon_incomplete",
                        message=retry_result.message or "Incomplete Amazon price sample.",
                    )
                    price_fetch_warnings.append(warning)

                    if warning.warning_type == "amazon_fetch_failed":
                        rows_failed_fetch += 1

                    print(f"  WARNING: {warning.message}")

                price_result = summarise_prices(retry_result.prices)

            except Exception as e:
                # Defensive catch: the retry helper should normally turn
                # per-attempt problems into structured warnings, but this keeps
                # the batch alive if an unexpected error escapes.
                #
                # If ons_price exists, the row can still receive an embodied
                # CO2 calculation using that curated replacement price.
                fetch_error = e
                rows_failed_fetch += 1

                warning = PriceFetchWarning(
                    item_name=item.item_name,
                    raw_search_term=raw_search_term,
                    cleaned_search_term=search_term,
                    prices_found=0,
                    min_prices_required=args.min_prices,
                    attempts_used=0,
                    max_retries=args.max_retries,
                    warning_type="amazon_unexpected_error",
                    message=str(e),
                )
                price_fetch_warnings.append(warning)

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
                defra_spend_factor_CO2=item.defra_spend_factor_CO2,
            )

            if replacement_cost_adjusted is None:
                rows_without_price_basis += 1

            if embodied_CO2_kg is None:
                rows_without_embodied_CO2 += 1

            if fetch_error is None and len(price_result.prices) >= args.min_prices:
                notes = (
                    "Automated Amazon UK search-price sample using cleaned search term "
                    f"{search_term!r}, derived from raw search term {raw_search_term!r}. "
                    "Search term comes from item_dictionary.price_search_term if provided; "
                    "otherwise item_dictionary.item_description. "
                    f"Amazon QC passed: parsed {len(price_result.prices)} price(s), "
                    f"minimum required {args.min_prices}. "
                    "amazon_price_upper = amazon_price_mean + amazon_price_std. "
                    "replacement_cost_adjusted uses ons_price if available; "
                    "otherwise amazon_price_upper. "
                    "embodied_CO2_kg = replacement_cost_adjusted * defra_spend_factor_CO2 * 0.5."
                )
            elif fetch_error is None:
                warning_message = (
                    retry_result.message
                    if retry_result is not None and retry_result.message is not None
                    else "Amazon price retrieval did not meet the configured minimum."
                )

                notes = (
                    "WARNING: Automated Amazon UK search-price sample is incomplete. "
                    f"Parsed {len(price_result.prices)} price(s); required "
                    f"{args.min_prices}; max_retries={args.max_retries}. "
                    f"Cleaned search term {search_term!r}, derived from raw search term "
                    f"{raw_search_term!r}. "
                    f"Warning detail: {warning_message}. "
                    "amazon_price_upper = amazon_price_mean + amazon_price_std, "
                    "but this estimate may be less reliable because fewer than the "
                    "target number of prices were available. "
                    "replacement_cost_adjusted uses ons_price if available; "
                    "otherwise amazon_price_upper. "
                    "embodied_CO2_kg = replacement_cost_adjusted * "
                    "defra_spend_factor_CO2 * 0.5."
                )
            else:
                notes = (
                    f"Amazon price fetch failed using cleaned search term {search_term!r}, "
                    f"derived from raw search term {raw_search_term!r}: {fetch_error}. "
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

            # Avoid overwriting a previously useful row with an entirely empty
            # Amazon result when there is no curated ONS/reference price to fall
            # back on. Partial non-empty Amazon samples are still written, but
            # clearly marked in the row notes and the final structured warning
            # summary.
            should_write_row = True

            if len(price_result.prices) == 0 and item.ons_price is None:
                should_write_row = False
                rows_skipped_due_to_price_fetch += 1

                print(
                    "  Skipping database write because no Amazon prices were parsed "
                    "and no ONS/reference price is available."
                )

            if not args.dry_run and should_write_row:
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
    print(f"  Incomplete Amazon samples:  {rows_with_incomplete_amazon_sample}")
    print(f"  Rows skipped due to price fetch: {rows_skipped_due_to_price_fetch}")
    print(f"  Price retrieval warnings:   {len(price_fetch_warnings)}")

    if price_fetch_warnings:
        print("\nStructured Amazon price retrieval warnings:")
        print(
            "  item_name | warning_type | prices_found | min_required | "
            "attempts_used | cleaned_search_term | message"
        )

        for warning in price_fetch_warnings:
            print(
                "  "
                f"{warning.item_name} | "
                f"{warning.warning_type} | "
                f"{warning.prices_found} | "
                f"{warning.min_prices_required} | "
                f"{warning.attempts_used}/{warning.max_retries} | "
                f"{warning.cleaned_search_term!r} | "
                f"{warning.message}"
            )

    if args.dry_run:
        print("\nDry run only: no database rows were written.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())