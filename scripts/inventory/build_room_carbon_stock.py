# scripts/inventory/build_room_carbon_stock.py
"""
Build room-level direct carbon stock and replacement embodied CO2 summaries.

This script rebuilds two intermediate modelling tables in the shared inventory
SQLite database:

    - room_carbon_stock
    - room_embodied_CO2

The two outputs are deliberately kept as separate tables because they describe
different physical / accounting quantities:

    room_carbon_stock
        Direct combustion carbon stock present in the room.
        Units: kgC.
        Built from item count * item mass * material carbon fraction.

    room_embodied_CO2
        Maximum fire-attributable replacement embodied CO2 if the room contents
        are fully replaced.
        Units: kg CO2.
        Built from item count * embodied_CO2_kg per item.

The two tables are built together because they use the same upstream count
summaries, assumed-inventory logic, and comparison-room logic. Building them in
one transaction avoids the model seeing a fresh direct-carbon table but a stale
embodied-CO2 table.

Current scope:
    (i) Build room-level expected/q25/q75 direct carbon stock from
        item_count_summary.
    (ii) Build room-level expected/q25/q75 replacement embodied CO2 from
         item_count_summary and embodied_carbon_data.
    (iii) Optionally include assumed_inventory contributions, using the same
          --assumed include/exclude behaviour as before.
    (iv) Add comparison-derived room types using room.room_type_comp_* metadata.
    (v) Exclude non-archetypal fire-input categories such as 'unknown'.

Important interpretation notes:
    - The q25 and q75 room-level values are built by summing item-level q25 /
      q75-derived estimates across the room.
    - They are compact descriptive room summaries, not full joint room-level
      quantiles from a Monte Carlo simulation of room contents.
    - embodied_carbon_data.embodied_CO2_kg is interpreted as the fire-
      attributable replacement embodied CO2 for one item unit. It is not a
      kg-item-normalised emission intensity.
    - Missing embodied_CO2_kg values are treated as a hard failure, because
      silently skipping them would undercount replacement emissions in the fire
      model.

This is an intermediate modelling step, not the final fire/emissions
calculation step. The resulting tables are intended to be reused by downstream
inventory snapshots and fire-emissions modelling scripts.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from scripts.db_lock import acquire_lock, release_lock, DatabaseLockedError


# Exclude non-archetypal room categories that may be useful elsewhere in the
# project, but should not contribute towards inventory-derived room summaries.
#
# Current example:
#   unknown = fire event input uncertainty category, not a modelled room
#             archetype with contents.
EXCLUDED_ROOM_TYPES = {
    "unknown",
}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def build_room_carbon_stock(
    db_path: Path,
    *,
    assumed: str = "include",
) -> dict:
    """
    Rebuild room_carbon_stock and room_embodied_CO2 from scratch.

    Workflow:
        1) Acquire DB lock, because the shared DB may be used by collaborators.
        2) Validate required source and target tables.
        3) Check upstream source data are present.
        4) Clear both target summary tables.
        5) Rebuild direct carbon and embodied CO2 room summaries together.
        6) Commit both tables in one transaction.
        7) Release DB lock.

    Parameters
    ----------
    db_path:
        Path to the inventory SQLite database.

    assumed:
        Controls whether curated assumed_inventory rows are included.

        include:
            Add assumed_inventory rows into both room_carbon_stock and
            room_embodied_CO2.

        exclude:
            Build both tables from survey-derived item_count_summary only.

    Returns
    -------
    dict
        Compact summary for printing by scripts/model.py.
    """

    if assumed not in {"include", "exclude"}:
        raise ValueError("assumed must be either 'include' or 'exclude'")

    lock = None

    try:
        lock = acquire_lock(
            db_path,
            purpose="build room carbon stock and embodied CO2",
        )
        conn = sqlite3.connect(db_path)

        try:
            # Use foreign keys consistently, as both target tables reference
            # curated room vocabulary rows.
            conn.execute("PRAGMA foreign_keys = ON")

            print("\nValidating required tables...")
            validate_room_stock_tables(conn, assumed=assumed)

            print("Checking source data are present...")
            check_room_stock_source_data_present(conn, assumed=assumed)

            print("Clearing existing room stock summary tables...")
            clear_room_stock_tables(conn)

            print("Rebuilding room carbon stock and embodied CO2 summaries...")
            summary = rebuild_room_stock_tables(conn, assumed=assumed)

            conn.commit()
            return summary

        except Exception:
            # Avoid leaving one summary table updated while the other remains
            # stale. If anything fails, both target-table changes are rolled
            # back together.
            conn.rollback()
            raise

        finally:
            conn.close()

    except DatabaseLockedError:
        raise

    finally:
        if lock is not None:
            release_lock(lock)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def validate_room_stock_tables(
    conn: sqlite3.Connection,
    *,
    assumed: str = "include",
) -> None:
    """
    Check that all required source and target tables exist and contain the
    columns expected by this script.

    Required source tables:
        - item_count_summary
        - item_dictionary
        - furniture
        - embodied_carbon_data
        - room
        - assumed_inventory and room_count_summary, if assumed == 'include'

    Required target tables:
        - room_carbon_stock
        - room_embodied_CO2
    """

    required_tables = {
        "item_count_summary",
        "item_dictionary",
        "furniture",
        "embodied_carbon_data",
        "room",
        "room_carbon_stock",
        "room_embodied_CO2",
    }

    if assumed == "include":
        required_tables.update({
            "assumed_inventory",
            "room_count_summary",
        })

    cur = conn.cursor()
    cur.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
    """)
    existing_tables = {row[0] for row in cur.fetchall()}

    missing = sorted(required_tables - existing_tables)
    if missing:
        raise RuntimeError(
            "Required table(s) missing from database:\n"
            + "\n".join(f"  - {name}" for name in missing)
            + "\n\nInitialise / update the DB schema first before running model.py."
        )

    require_columns(
        conn,
        table_name="item_count_summary",
        required_columns={
            "item_name",
            "room_type",
            "expected_count_mean",
            "count_q25",
            "count_q75",
        },
    )

    require_columns(
        conn,
        table_name="item_dictionary",
        required_columns={
            "item_name",
            "item_mass",
            "furniture_class",
        },
    )

    require_columns(
        conn,
        table_name="furniture",
        required_columns={
            "furniture_class",
            "kgC_kg",
            "ratio_fossil",
            "ratio_biog",
        },
    )

    require_columns(
        conn,
        table_name="embodied_carbon_data",
        required_columns={
            "item_name",
            "embodied_CO2_kg",
        },
    )

    require_columns(
        conn,
        table_name="room",
        required_columns={
            "room_type",
            "room_type_comp_1",
            "room_type_comp_2",
            "room_type_comp_ratio",
        },
    )

    require_columns(
        conn,
        table_name="room_carbon_stock",
        required_columns={
            "room_type",
            "expected_total_carbon_kgC",
            "expected_biog_carbon_kgC",
            "expected_fossil_carbon_kgC",
            "q25_total_carbon_kgC",
            "q25_biog_carbon_kgC",
            "q25_fossil_carbon_kgC",
            "q75_total_carbon_kgC",
            "q75_biog_carbon_kgC",
            "q75_fossil_carbon_kgC",
            "carbon_notes",
        },
    )

    require_columns(
        conn,
        table_name="room_embodied_CO2",
        required_columns={
            "room_type",
            "expected_embodied_CO2_kg",
            "q25_embodied_CO2_kg",
            "q75_embodied_CO2_kg",
            "embodied_CO2_notes",
        },
    )

    if assumed == "include":
        require_columns(
            conn,
            table_name="assumed_inventory",
            required_columns={
                "room_type",
                "item_name",
                "count_assumed",
                "dependency",
                "dependency_type",
                "dependency_quantifier",
                "assumption_notes",
            },
        )

        require_columns(
            conn,
            table_name="room_count_summary",
            required_columns={
                "room_type",
                "expected_count_mean",
                "count_q25",
                "count_q75",
            },
        )


def check_room_stock_source_data_present(
    conn: sqlite3.Connection,
    *,
    assumed: str = "include",
) -> None:
    """
    Check that upstream source tables contain rows.

    This distinguishes a valid schema with no model inputs from a missing or
    out-of-date schema. Missing item-level embodied CO2 values are checked later
    row-by-row so that the error can name the affected item.
    """

    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM item_count_summary")
    item_summary_rows = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM item_dictionary")
    item_dictionary_rows = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM furniture")
    furniture_rows = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM embodied_carbon_data")
    embodied_rows = cur.fetchone()[0]

    if item_summary_rows == 0:
        raise RuntimeError(
            "No source rows found in item_count_summary.\n\n"
            "Run the inventory distribution step first, then rerun room_carbon."
        )

    if item_dictionary_rows == 0:
        raise RuntimeError(
            "No source rows found in item_dictionary.\n\n"
            "Ingest the vocab/item mapping data first, then rerun room_carbon."
        )

    if furniture_rows == 0:
        raise RuntimeError(
            "No source rows found in furniture.\n\n"
            "Ingest the vocab/furniture class data first, then rerun room_carbon."
        )

    if embodied_rows == 0:
        raise RuntimeError(
            "No source rows found in embodied_carbon_data.\n\n"
            "Run scripts.lca.fetch_amazon_prices first, then rerun room_carbon."
        )

    if assumed == "include":
        cur.execute("SELECT COUNT(*) FROM assumed_inventory")
        assumed_inventory_rows = cur.fetchone()[0]

        if assumed_inventory_rows == 0:
            raise RuntimeError(
                "No source rows found in assumed_inventory.\n\n"
                "Run the assumed items ingester first, or rerun room_carbon with:\n"
                "  --assumed exclude"
            )


# ---------------------------------------------------------------------------
# Target-table clearing
# ---------------------------------------------------------------------------

def clear_room_stock_tables(conn: sqlite3.Connection) -> None:
    """
    Delete existing rows from both target summary tables.

    The room stock tables are intermediate derived summaries. Full rebuilds are
    easier to reason about than incremental updates, and the row count is small.
    Both tables are cleared in the same transaction so they remain aligned.
    """

    cur = conn.cursor()
    cur.execute("DELETE FROM room_carbon_stock")
    cur.execute("DELETE FROM room_embodied_CO2")


# ---------------------------------------------------------------------------
# Direct-carbon accumulator helpers
# ---------------------------------------------------------------------------

def ensure_room_carbon_accumulator(room_totals: dict, room_type: str) -> None:
    """
    Ensure room_totals contains a direct-carbon accumulator for room_type.

    The accumulator stores separate mean/q25/q75 direct carbon-stock metrics,
    each split into total, biogenic, and fossil carbon.
    """

    if room_type not in room_totals:
        room_totals[room_type] = {
            "expected_total_carbon_kgC": 0.0,
            "expected_biog_carbon_kgC": 0.0,
            "expected_fossil_carbon_kgC": 0.0,
            "q25_total_carbon_kgC": 0.0,
            "q25_biog_carbon_kgC": 0.0,
            "q25_fossil_carbon_kgC": 0.0,
            "q75_total_carbon_kgC": 0.0,
            "q75_biog_carbon_kgC": 0.0,
            "q75_fossil_carbon_kgC": 0.0,
        }


def add_item_carbon_to_room_totals(
    *,
    room_totals: dict,
    room_type: str,
    count_mean,
    count_q25,
    count_q75,
    item_mass,
    kgC_kg,
    ratio_fossil,
    ratio_biog,
) -> bool:
    """
    Add one item category's direct-carbon contribution to a room accumulator.

    Calculation repeated independently for mean, q25, and q75 counts:

        item mass in room = item count * item_mass
        total carbon      = item mass in room * kgC_kg
        fossil carbon     = total carbon * ratio_fossil
        biogenic carbon   = total carbon * ratio_biog

    Inputs may come from:
        - empirical item_count_summary rows; or
        - effective assumed_inventory counts after dependency adjustment.

    Returns True if any of mean/q25/q75 contributes a positive value.
    """

    ensure_room_carbon_accumulator(room_totals, room_type)
    metric_contributed = False

    # Mean-based direct carbon stock.
    if count_mean is not None and count_mean > 0:
        expected_item_mass_kg = float(count_mean) * float(item_mass)
        expected_total_carbon_kgC = expected_item_mass_kg * float(kgC_kg)
        expected_fossil_carbon_kgC = expected_total_carbon_kgC * float(ratio_fossil)
        expected_biog_carbon_kgC = expected_total_carbon_kgC * float(ratio_biog)

        room_totals[room_type]["expected_total_carbon_kgC"] += expected_total_carbon_kgC
        room_totals[room_type]["expected_fossil_carbon_kgC"] += expected_fossil_carbon_kgC
        room_totals[room_type]["expected_biog_carbon_kgC"] += expected_biog_carbon_kgC
        metric_contributed = True

    # Lower descriptive direct carbon stock.
    if count_q25 is not None and count_q25 > 0:
        q25_item_mass_kg = float(count_q25) * float(item_mass)
        q25_total_carbon_kgC = q25_item_mass_kg * float(kgC_kg)
        q25_fossil_carbon_kgC = q25_total_carbon_kgC * float(ratio_fossil)
        q25_biog_carbon_kgC = q25_total_carbon_kgC * float(ratio_biog)

        room_totals[room_type]["q25_total_carbon_kgC"] += q25_total_carbon_kgC
        room_totals[room_type]["q25_fossil_carbon_kgC"] += q25_fossil_carbon_kgC
        room_totals[room_type]["q25_biog_carbon_kgC"] += q25_biog_carbon_kgC
        metric_contributed = True

    # Upper descriptive direct carbon stock.
    if count_q75 is not None and count_q75 > 0:
        q75_item_mass_kg = float(count_q75) * float(item_mass)
        q75_total_carbon_kgC = q75_item_mass_kg * float(kgC_kg)
        q75_fossil_carbon_kgC = q75_total_carbon_kgC * float(ratio_fossil)
        q75_biog_carbon_kgC = q75_total_carbon_kgC * float(ratio_biog)

        room_totals[room_type]["q75_total_carbon_kgC"] += q75_total_carbon_kgC
        room_totals[room_type]["q75_fossil_carbon_kgC"] += q75_fossil_carbon_kgC
        room_totals[room_type]["q75_biog_carbon_kgC"] += q75_biog_carbon_kgC
        metric_contributed = True

    return metric_contributed


# ---------------------------------------------------------------------------
# Embodied-CO2 accumulator helpers
# ---------------------------------------------------------------------------

def ensure_room_embodied_CO2_accumulator(room_totals: dict, room_type: str) -> None:
    """
    Ensure room_totals contains an embodied-CO2 accumulator for room_type.

    The accumulator stores maximum full-room replacement embodied CO2 for the
    mean, q25, and q75 item-count summaries.
    """

    if room_type not in room_totals:
        room_totals[room_type] = {
            "expected_embodied_CO2_kg": 0.0,
            "q25_embodied_CO2_kg": 0.0,
            "q75_embodied_CO2_kg": 0.0,
        }


def add_item_embodied_CO2_to_room_totals(
    *,
    room_totals: dict,
    room_type: str,
    count_mean,
    count_q25,
    count_q75,
    embodied_CO2_kg,
) -> bool:
    """
    Add one item category's replacement embodied-CO2 contribution to a room.

    Calculation repeated independently for mean, q25, and q75 counts:

        room embodied CO2 contribution = item count * embodied_CO2_kg

    where:
        embodied_CO2_kg
            Fire-attributable replacement embodied CO2 for one item unit, as
            calculated by scripts.lca.fetch_amazon_prices and stored in
            embodied_carbon_data.

    This value is already an item-level kg CO2 result. Do not multiply it by
    item mass again.

    Returns True if any of mean/q25/q75 contributes a positive value.
    """

    if embodied_CO2_kg is None:
        raise RuntimeError(
            "NULL embodied_CO2_kg encountered while building room_embodied_CO2."
        )

    ensure_room_embodied_CO2_accumulator(room_totals, room_type)
    metric_contributed = False

    # Mean-based maximum replacement embodied CO2 for this item category.
    if count_mean is not None and count_mean > 0:
        room_totals[room_type]["expected_embodied_CO2_kg"] += (
            float(count_mean) * float(embodied_CO2_kg)
        )
        metric_contributed = True

    # Lower descriptive replacement embodied CO2.
    if count_q25 is not None and count_q25 > 0:
        room_totals[room_type]["q25_embodied_CO2_kg"] += (
            float(count_q25) * float(embodied_CO2_kg)
        )
        metric_contributed = True

    # Upper descriptive replacement embodied CO2.
    if count_q75 is not None and count_q75 > 0:
        room_totals[room_type]["q75_embodied_CO2_kg"] += (
            float(count_q75) * float(embodied_CO2_kg)
        )
        metric_contributed = True

    return metric_contributed


# ---------------------------------------------------------------------------
# Assumed-inventory helper
# ---------------------------------------------------------------------------

def add_assumed_inventory_to_room_totals(
    conn: sqlite3.Connection,
    carbon_totals: dict,
    embodied_totals: dict,
) -> dict:
    """
    Add assumed_inventory contributions into both room-level accumulators.

    assumed_inventory rows represent curated model assumptions, not empirical
    survey responses. They are added after survey-derived item_count_summary
    contributions and before comparison-derived room rows.

    Effective count calculation for each assumed item:

        effective mean count =
            count_assumed + dependency_quantifier * dependency mean count

        effective q25 count =
            count_assumed + dependency_quantifier * dependency q25 count

        effective q75 count =
            count_assumed + dependency_quantifier * dependency q75 count

    Dependency counts are read from:
        - item_count_summary, when dependency_type == 'item_name'
        - room_count_summary, when dependency_type == 'room_type'

    The same effective counts are then used for both outputs:
        - direct carbon stock: count * item_mass * kgC_kg
        - embodied CO2 stock: count * embodied_CO2_kg
    """

    cur = conn.cursor()
    placeholders = ", ".join("?" for _ in sorted(EXCLUDED_ROOM_TYPES))

    cur.execute(f"""
        SELECT
            ai.room_type,
            ai.item_name,
            ai.count_assumed,
            ai.dependency,
            ai.dependency_type,
            ai.dependency_quantifier,
            id.item_mass,
            id.furniture_class,
            fc.kgC_kg,
            fc.ratio_fossil,
            fc.ratio_biog,
            ecd.embodied_CO2_kg
        FROM assumed_inventory AS ai
        JOIN item_dictionary AS id
            ON ai.item_name = id.item_name
        JOIN furniture AS fc
            ON id.furniture_class = fc.furniture_class
        LEFT JOIN embodied_carbon_data AS ecd
            ON ai.item_name = ecd.item_name
        WHERE ai.room_type NOT IN ({placeholders})
        ORDER BY ai.room_type, ai.item_name
    """, tuple(sorted(EXCLUDED_ROOM_TYPES)))

    assumed_rows = cur.fetchall()

    assumed_rows_contributing_carbon = 0
    assumed_rows_contributing_embodied = 0

    for row in assumed_rows:
        (
            room_type,
            item_name,
            count_assumed,
            dependency,
            dependency_type,
            dependency_quantifier,
            item_mass,
            furniture_class,
            kgC_kg,
            ratio_fossil,
            ratio_biog,
            embodied_CO2_kg,
        ) = row

        validate_item_stock_inputs(
            item_name=item_name,
            furniture_class=furniture_class,
            item_mass=item_mass,
            kgC_kg=kgC_kg,
            ratio_fossil=ratio_fossil,
            ratio_biog=ratio_biog,
            embodied_CO2_kg=embodied_CO2_kg,
            context="assumed_inventory",
        )

        count_mean, count_q25, count_q75 = resolve_assumed_effective_counts(
            conn,
            room_type=room_type,
            item_name=item_name,
            count_assumed=count_assumed,
            dependency=dependency,
            dependency_type=dependency_type,
            dependency_quantifier=dependency_quantifier,
        )

        contributed_carbon = add_item_carbon_to_room_totals(
            room_totals=carbon_totals,
            room_type=room_type,
            count_mean=count_mean,
            count_q25=count_q25,
            count_q75=count_q75,
            item_mass=item_mass,
            kgC_kg=kgC_kg,
            ratio_fossil=ratio_fossil,
            ratio_biog=ratio_biog,
        )

        contributed_embodied = add_item_embodied_CO2_to_room_totals(
            room_totals=embodied_totals,
            room_type=room_type,
            count_mean=count_mean,
            count_q25=count_q25,
            count_q75=count_q75,
            embodied_CO2_kg=embodied_CO2_kg,
        )

        if contributed_carbon:
            assumed_rows_contributing_carbon += 1
        if contributed_embodied:
            assumed_rows_contributing_embodied += 1

    return {
        "assumed_rows": len(assumed_rows),
        "assumed_rows_contributing_carbon": assumed_rows_contributing_carbon,
        "assumed_rows_contributing_embodied": assumed_rows_contributing_embodied,
    }


def resolve_assumed_effective_counts(
    conn: sqlite3.Connection,
    *,
    room_type: str,
    item_name: str,
    count_assumed,
    dependency,
    dependency_type,
    dependency_quantifier,
) -> tuple[float, float, float]:
    """
    Resolve mean/q25/q75 effective counts for one assumed_inventory row.

    This helper isolates the dependency logic so the resolved counts can be
    reused for both direct-carbon and embodied-CO2 calculations.
    """

    if count_assumed is None:
        raise RuntimeError(
            f"NULL count_assumed encountered for assumed item_name='{item_name}' "
            f"in room_type='{room_type}'."
        )

    count_mean = float(count_assumed)
    count_q25 = float(count_assumed)
    count_q75 = float(count_assumed)

    if dependency_type is None:
        return count_mean, count_q25, count_q75

    if dependency_quantifier is None:
        raise RuntimeError(
            "Assumed inventory dependency has dependency_type but no "
            "dependency_quantifier:\n"
            f"  assumed item: {item_name}\n"
            f"  assumed room: {room_type}\n"
            f"  dependency_type: {dependency_type}\n"
            f"  dependency: {dependency}"
        )

    dep_multiplier = float(dependency_quantifier)
    cur = conn.cursor()

    if dependency_type == "item_name":
        cur.execute("""
            SELECT
                expected_count_mean,
                count_q25,
                count_q75
            FROM item_count_summary
            WHERE item_name = ?
              AND room_type = ?
        """, (
            dependency,
            room_type,
        ))

        dep_row = cur.fetchone()

        if dep_row is None:
            raise RuntimeError(
                "Assumed inventory dependency could not be resolved in "
                "item_count_summary:\n"
                f"  assumed item: {item_name}\n"
                f"  assumed room: {room_type}\n"
                f"  dependency item_name: {dependency}\n\n"
                "Run the inventory model first, or check that the dependency "
                "item appears in item_count_summary for the same room_type."
            )

    elif dependency_type == "room_type":
        cur.execute("""
            SELECT
                expected_count_mean,
                count_q25,
                count_q75
            FROM room_count_summary
            WHERE room_type = ?
        """, (
            dependency,
        ))

        dep_row = cur.fetchone()

        if dep_row is None:
            raise RuntimeError(
                "Assumed inventory dependency could not be resolved in "
                "room_count_summary:\n"
                f"  assumed item: {item_name}\n"
                f"  assumed room: {room_type}\n"
                f"  dependency room_type: {dependency}\n\n"
                "Run the inventory model first, or check that the dependency "
                "room_type appears in room_count_summary."
            )

    else:
        raise RuntimeError(
            f"Unexpected dependency_type='{dependency_type}' for assumed "
            f"item_name='{item_name}'."
        )

    dep_mean, dep_q25, dep_q75 = dep_row

    if dep_mean is not None:
        count_mean += dep_multiplier * float(dep_mean)
    if dep_q25 is not None:
        count_q25 += dep_multiplier * float(dep_q25)
    if dep_q75 is not None:
        count_q75 += dep_multiplier * float(dep_q75)

    return count_mean, count_q25, count_q75


# ---------------------------------------------------------------------------
# Comparison-derived room wrappers
# ---------------------------------------------------------------------------

def fetch_comparison_room_rows(conn: sqlite3.Connection) -> list[tuple]:
    """
    Fetch room rows that define derived/comparison room summaries.

    Current derivation rule used by both wrappers:

        derived room total = (comp_1 total + comp_2 total) * comp_ratio

    where comp_2 is optional. If comp_2 is NULL or missing from the accumulator,
    it contributes zero.
    """

    cur = conn.cursor()
    placeholders = ", ".join("?" for _ in sorted(EXCLUDED_ROOM_TYPES))

    cur.execute(f"""
        SELECT
            room_type,
            room_type_comp_1,
            room_type_comp_2,
            room_type_comp_ratio
        FROM room
        WHERE room_type NOT IN ({placeholders})
          AND room_type_comp_1 IS NOT NULL
          AND room_type_comp_ratio IS NOT NULL
        ORDER BY room_type
    """, tuple(sorted(EXCLUDED_ROOM_TYPES)))

    return cur.fetchall()


def add_comparison_room_carbon_totals(
    conn: sqlite3.Connection,
    room_totals: dict,
) -> dict:
    """
    Add comparison-derived direct-carbon rows using room_type_comp_* metadata.

    The derivation is applied independently to every direct-carbon metric:
        - expected total/biogenic/fossil carbon
        - q25 total/biogenic/fossil carbon
        - q75 total/biogenic/fossil carbon
    """

    comparison_rows = fetch_comparison_room_rows(conn)

    zero_totals = {
        "expected_total_carbon_kgC": 0.0,
        "expected_biog_carbon_kgC": 0.0,
        "expected_fossil_carbon_kgC": 0.0,
        "q25_total_carbon_kgC": 0.0,
        "q25_biog_carbon_kgC": 0.0,
        "q25_fossil_carbon_kgC": 0.0,
        "q75_total_carbon_kgC": 0.0,
        "q75_biog_carbon_kgC": 0.0,
        "q75_fossil_carbon_kgC": 0.0,
    }

    eligible_rows = 0
    derived_rows_added = 0
    skipped_missing_comp_1 = 0

    for room_type, comp_1, comp_2, comp_ratio in comparison_rows:
        eligible_rows += 1

        if comp_1 not in room_totals:
            skipped_missing_comp_1 += 1
            continue

        base_1 = room_totals[comp_1]
        base_2 = room_totals[comp_2] if comp_2 is not None and comp_2 in room_totals else zero_totals
        ratio = float(comp_ratio)

        room_totals[room_type] = {
            metric_name: (base_1[metric_name] + base_2[metric_name]) * ratio
            for metric_name in zero_totals
        }

        derived_rows_added += 1

    return {
        "comparison_rows_eligible": eligible_rows,
        "comparison_rows_added": derived_rows_added,
        "comparison_rows_skipped_missing_comp_1": skipped_missing_comp_1,
    }


def add_comparison_room_embodied_CO2_totals(
    conn: sqlite3.Connection,
    room_totals: dict,
) -> dict:
    """
    Add comparison-derived embodied-CO2 rows using room_type_comp_* metadata.

    This is intentionally a separate wrapper from direct carbon so that the
    units and output metric names remain obvious.
    """

    comparison_rows = fetch_comparison_room_rows(conn)

    zero_totals = {
        "expected_embodied_CO2_kg": 0.0,
        "q25_embodied_CO2_kg": 0.0,
        "q75_embodied_CO2_kg": 0.0,
    }

    eligible_rows = 0
    derived_rows_added = 0
    skipped_missing_comp_1 = 0

    for room_type, comp_1, comp_2, comp_ratio in comparison_rows:
        eligible_rows += 1

        if comp_1 not in room_totals:
            skipped_missing_comp_1 += 1
            continue

        base_1 = room_totals[comp_1]
        base_2 = room_totals[comp_2] if comp_2 is not None and comp_2 in room_totals else zero_totals
        ratio = float(comp_ratio)

        room_totals[room_type] = {
            metric_name: (base_1[metric_name] + base_2[metric_name]) * ratio
            for metric_name in zero_totals
        }

        derived_rows_added += 1

    return {
        "comparison_rows_eligible": eligible_rows,
        "comparison_rows_added": derived_rows_added,
        "comparison_rows_skipped_missing_comp_1": skipped_missing_comp_1,
    }


# ---------------------------------------------------------------------------
# Main rebuild logic
# ---------------------------------------------------------------------------

def rebuild_room_stock_tables(
    conn: sqlite3.Connection,
    *,
    assumed: str = "include",
) -> dict:
    """
    Rebuild room_carbon_stock and room_embodied_CO2 together.

    Main observed/survey-derived source:
        item_count_summary

    For each observed item_name x room_type row:
        1) Read expected_count_mean, count_q25, count_q75.
        2) Read item_mass and furniture_class from item_dictionary.
        3) Read kgC_kg, ratio_fossil, ratio_biog from furniture.
        4) Read embodied_CO2_kg from embodied_carbon_data.
        5) Add direct carbon contribution:
               count * item_mass * kgC_kg
           then split into fossil/biogenic carbon.
        6) Add embodied CO2 contribution:
               count * embodied_CO2_kg
           without multiplying by item_mass.

    If assumed == 'include', assumed_inventory rows are then resolved to
    effective counts and added to both accumulators.

    Finally, comparison-derived room rows are added to both accumulators and
    both output tables are written.
    """

    cur = conn.cursor()
    placeholders = ", ".join("?" for _ in sorted(EXCLUDED_ROOM_TYPES))

    cur.execute(f"""
        SELECT
            ics.room_type,
            ics.item_name,
            ics.expected_count_mean,
            ics.count_q25,
            ics.count_q75,
            id.item_mass,
            id.furniture_class,
            fc.kgC_kg,
            fc.ratio_fossil,
            fc.ratio_biog,
            ecd.embodied_CO2_kg
        FROM item_count_summary AS ics
        JOIN item_dictionary AS id
            ON ics.item_name = id.item_name
        JOIN furniture AS fc
            ON id.furniture_class = fc.furniture_class
        LEFT JOIN embodied_carbon_data AS ecd
            ON ics.item_name = ecd.item_name
        WHERE ics.room_type NOT IN ({placeholders})
        ORDER BY ics.room_type, ics.item_name
    """, tuple(sorted(EXCLUDED_ROOM_TYPES)))

    source_rows = cur.fetchall()

    if not source_rows:
        raise RuntimeError(
            "No eligible item_count_summary rows found after excluding restricted "
            "room types.\n\n"
            "Run the upstream inventory model first, or check whether the summary "
            "table contains any non-excluded room categories."
        )

    carbon_totals = {}
    embodied_totals = {}

    contributing_item_rows_carbon = 0
    contributing_item_rows_embodied = 0

    # ------------------------------------------------------------------
    # Add observed/survey-derived item contributions.
    # ------------------------------------------------------------------
    for row in source_rows:
        (
            room_type,
            item_name,
            expected_count_mean,
            count_q25,
            count_q75,
            item_mass,
            furniture_class,
            kgC_kg,
            ratio_fossil,
            ratio_biog,
            embodied_CO2_kg,
        ) = row

        validate_item_stock_inputs(
            item_name=item_name,
            furniture_class=furniture_class,
            item_mass=item_mass,
            kgC_kg=kgC_kg,
            ratio_fossil=ratio_fossil,
            ratio_biog=ratio_biog,
            embodied_CO2_kg=embodied_CO2_kg,
            context="item_count_summary",
        )

        contributed_carbon = add_item_carbon_to_room_totals(
            room_totals=carbon_totals,
            room_type=room_type,
            count_mean=expected_count_mean,
            count_q25=count_q25,
            count_q75=count_q75,
            item_mass=item_mass,
            kgC_kg=kgC_kg,
            ratio_fossil=ratio_fossil,
            ratio_biog=ratio_biog,
        )

        contributed_embodied = add_item_embodied_CO2_to_room_totals(
            room_totals=embodied_totals,
            room_type=room_type,
            count_mean=expected_count_mean,
            count_q25=count_q25,
            count_q75=count_q75,
            embodied_CO2_kg=embodied_CO2_kg,
        )

        if contributed_carbon:
            contributing_item_rows_carbon += 1
        if contributed_embodied:
            contributing_item_rows_embodied += 1

    # ------------------------------------------------------------------
    # Optionally add curated assumed_inventory contributions.
    # ------------------------------------------------------------------
    assumed_summary = {
        "assumed_rows": 0,
        "assumed_rows_contributing_carbon": 0,
        "assumed_rows_contributing_embodied": 0,
    }

    if assumed == "include":
        assumed_summary = add_assumed_inventory_to_room_totals(
            conn,
            carbon_totals,
            embodied_totals,
        )

    # ------------------------------------------------------------------
    # Add comparison-derived room summaries.
    # ------------------------------------------------------------------
    carbon_comparison_summary = add_comparison_room_carbon_totals(
        conn,
        carbon_totals,
    )

    embodied_comparison_summary = add_comparison_room_embodied_CO2_totals(
        conn,
        embodied_totals,
    )

    # ------------------------------------------------------------------
    # Insert final room-level rows into separate output tables.
    # ------------------------------------------------------------------
    carbon_rows_written = insert_room_carbon_stock_rows(
        conn,
        carbon_totals,
        assumed=assumed,
    )

    embodied_rows_written = insert_room_embodied_CO2_rows(
        conn,
        embodied_totals,
        assumed=assumed,
    )

    return {
        "source_rows": len(source_rows),
        "contributing_item_rows_carbon": contributing_item_rows_carbon,
        "contributing_item_rows_embodied": contributing_item_rows_embodied,
        "assumed_inventory": assumed,
        "assumed_rows": assumed_summary["assumed_rows"],
        "assumed_rows_contributing_carbon": assumed_summary["assumed_rows_contributing_carbon"],
        "assumed_rows_contributing_embodied": assumed_summary["assumed_rows_contributing_embodied"],
        "carbon_comparison_rows_eligible": carbon_comparison_summary["comparison_rows_eligible"],
        "carbon_comparison_rows_added": carbon_comparison_summary["comparison_rows_added"],
        "carbon_comparison_rows_skipped_missing_comp_1": carbon_comparison_summary[
            "comparison_rows_skipped_missing_comp_1"
        ],
        "embodied_comparison_rows_eligible": embodied_comparison_summary["comparison_rows_eligible"],
        "embodied_comparison_rows_added": embodied_comparison_summary["comparison_rows_added"],
        "embodied_comparison_rows_skipped_missing_comp_1": embodied_comparison_summary[
            "comparison_rows_skipped_missing_comp_1"
        ],
        "room_carbon_rows_written": carbon_rows_written,
        "room_embodied_CO2_rows_written": embodied_rows_written,
    }


# Backwards-compatible alias for code that imports/calls the previous helper
# name directly. The public entry point build_room_carbon_stock() now calls the
# clearer rebuild_room_stock_tables() name above.
def rebuild_room_carbon_stock_table(
    conn: sqlite3.Connection,
    *,
    assumed: str = "include",
) -> dict:
    return rebuild_room_stock_tables(conn, assumed=assumed)


# ---------------------------------------------------------------------------
# Insert helpers
# ---------------------------------------------------------------------------

def insert_room_carbon_stock_rows(
    conn: sqlite3.Connection,
    room_totals: dict,
    *,
    assumed: str,
) -> int:
    """
    Insert accumulated direct-carbon room summaries into room_carbon_stock.
    """

    cur = conn.cursor()
    rows_written = 0

    for room_type in sorted(room_totals):
        totals = room_totals[room_type]

        cur.execute("""
            INSERT INTO room_carbon_stock (
                room_type,
                expected_total_carbon_kgC,
                expected_biog_carbon_kgC,
                expected_fossil_carbon_kgC,
                q25_total_carbon_kgC,
                q25_biog_carbon_kgC,
                q25_fossil_carbon_kgC,
                q75_total_carbon_kgC,
                q75_biog_carbon_kgC,
                q75_fossil_carbon_kgC,
                carbon_notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            room_type,
            totals["expected_total_carbon_kgC"],
            totals["expected_biog_carbon_kgC"],
            totals["expected_fossil_carbon_kgC"],
            totals["q25_total_carbon_kgC"],
            totals["q25_biog_carbon_kgC"],
            totals["q25_fossil_carbon_kgC"],
            totals["q75_total_carbon_kgC"],
            totals["q75_biog_carbon_kgC"],
            totals["q75_fossil_carbon_kgC"],
            (
                "Includes assumed_inventory contributions. Direct carbon stock "
                "is calculated as count * item_mass * kgC_kg and split using "
                "furniture.ratio_fossil / furniture.ratio_biog."
                if assumed == "include"
                else
                "Excludes assumed_inventory contributions. Direct carbon stock "
                "is calculated as count * item_mass * kgC_kg and split using "
                "furniture.ratio_fossil / furniture.ratio_biog."
            ),
        ))

        rows_written += 1

    return rows_written


def insert_room_embodied_CO2_rows(
    conn: sqlite3.Connection,
    room_totals: dict,
    *,
    assumed: str,
) -> int:
    """
    Insert accumulated replacement embodied-CO2 room summaries.

    The inserted values represent the maximum fire-attributable replacement
    embodied CO2 for each room_type if that room's contents are fully replaced.
    The fire model can later multiply these values by a replacement-area /
    damage fraction.
    """

    cur = conn.cursor()
    rows_written = 0

    for room_type in sorted(room_totals):
        totals = room_totals[room_type]

        cur.execute("""
            INSERT INTO room_embodied_CO2 (
                room_type,
                expected_embodied_CO2_kg,
                q25_embodied_CO2_kg,
                q75_embodied_CO2_kg,
                embodied_CO2_notes
            )
            VALUES (?, ?, ?, ?, ?)
        """, (
            room_type,
            totals["expected_embodied_CO2_kg"],
            totals["q25_embodied_CO2_kg"],
            totals["q75_embodied_CO2_kg"],
            (
                "Includes assumed_inventory contributions. Replacement embodied "
                "CO2 is calculated as count * embodied_carbon_data.embodied_CO2_kg. "
                "The item-level embodied_CO2_kg value is already fire-attributable "
                "and should not be multiplied by item_mass."
                if assumed == "include"
                else
                "Excludes assumed_inventory contributions. Replacement embodied "
                "CO2 is calculated as count * embodied_carbon_data.embodied_CO2_kg. "
                "The item-level embodied_CO2_kg value is already fire-attributable "
                "and should not be multiplied by item_mass."
            ),
        ))

        rows_written += 1

    return rows_written


# ---------------------------------------------------------------------------
# Defensive validation utilities
# ---------------------------------------------------------------------------

def validate_item_stock_inputs(
    *,
    item_name,
    furniture_class,
    item_mass,
    kgC_kg,
    ratio_fossil,
    ratio_biog,
    embodied_CO2_kg,
    context: str,
) -> None:
    """
    Fail fast if an item row is missing required stock-conversion inputs.

    Direct carbon requires:
        item_mass, kgC_kg, ratio_fossil, ratio_biog

    Replacement embodied CO2 requires:
        embodied_CO2_kg

    Missing embodied_CO2_kg is a hard failure because otherwise the room-level
    replacement emissions would be silently undercounted.
    """

    if item_mass is None:
        raise RuntimeError(
            f"NULL item_mass encountered for item_name='{item_name}' "
            f"while processing {context}."
        )
    if kgC_kg is None:
        raise RuntimeError(
            f"NULL kgC_kg encountered for furniture_class='{furniture_class}' "
            f"while processing item_name='{item_name}' from {context}."
        )
    if ratio_fossil is None:
        raise RuntimeError(
            f"NULL ratio_fossil encountered for furniture_class='{furniture_class}' "
            f"while processing item_name='{item_name}' from {context}."
        )
    if ratio_biog is None:
        raise RuntimeError(
            f"NULL ratio_biog encountered for furniture_class='{furniture_class}' "
            f"while processing item_name='{item_name}' from {context}."
        )
    if embodied_CO2_kg is None:
        raise RuntimeError(
            "Missing embodied_CO2_kg for item required by room_embodied_CO2:\n"
            f"  item_name: {item_name}\n"
            f"  context: {context}\n\n"
            "Run scripts.lca.fetch_amazon_prices first, then rerun room_carbon."
        )


def require_columns(
    conn: sqlite3.Connection,
    table_name: str,
    required_columns: set[str],
) -> None:
    """
    Validate that a table contains the expected columns.

    Helpful while the schema is still being refined, because table existence
    alone does not guarantee that the expected column names are present.
    """

    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    columns = {row[1] for row in cur.fetchall()}

    missing = sorted(required_columns - columns)
    if missing:
        raise RuntimeError(
            f"Table '{table_name}' is missing required column(s): "
            f"{', '.join(missing)}"
        )
