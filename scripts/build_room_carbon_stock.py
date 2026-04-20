# scripts/build_room_carbon_stock.py
"""
Build room-level carbon stock summary values in the shared SQLite database.

This script rebuilds the following intermediate modelling table from the
inventory-derived item count summaries already stored in the database:

    - room_carbon_stock

Current scope:
    (i) room-level expected carbon stock from item_count_summary
    (ii) restricted for now to:
         - kitchen
         - bedroom
         - living_room
         - unspecified_room

Important design choices for the current project stage:
    - Uses item_count_summary as the upstream source table
    - Uses expected_count_mean, count_q25, and count_q75 from that table
    - Converts expected item counts to expected item mass using item_dictionary.item_mass
    - Converts expected item mass to expected carbon mass using furniture_class.kgC_kg
    - Splits expected total carbon into fossil / biogenic components using
      furniture_class.ratio_fossil and furniture_class.ratio_biog
    - Rebuilds the target table from scratch each time (delete -> rebuild)
    - Leaves carbon_notes as NULL for now

Important interpretation note:
    The q25 and q75 room-level carbon values produced here are built by summing
    item-level q25 / q75-derived carbon estimates across the room.

    They are therefore compact descriptive room summaries derived from the
    item_count_summary table. They are not yet full joint room-level quantiles
    from a Monte Carlo simulation of room contents.

This is an intermediate modelling step, not the final fire/emissions calculation step.
The resulting table is intended to be reused later by downstream modelling scripts.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from scripts.db_lock import acquire_lock, release_lock, DatabaseLockedError


# Restrict the first-pass room carbon build to the main room categories
# currently used in the inventory model.
ALLOWED_ROOM_TYPES = {
    "bedroom",
    "kitchen",
    "living_room",
    "unspecified_room",
}


# Public function
def build_room_carbon_stock(db_path: Path) -> dict:
    """
    Rebuild the room_carbon_stock table from scratch.

    Workflow:
        1) Acquire DB lock (shared DB may be accessed by multiple collaborators)
        2) Validate required source / target tables exist
        3) Check source summary table contains data
        4) Delete old room carbon rows
        5) Rebuild room-level carbon stock summaries
        6) Commit changes and release lock

    Returns a compact summary dict for printing by scripts/model.py.
    """
    lock = None

    # Lock the database to prevent accidental simultaneous write (from db_lock.py)
    try:
        lock = acquire_lock(db_path, purpose="build room carbon stock")
        conn = sqlite3.connect(db_path)

        try:
            # Use foreign keys consistently, as target tables reference vocab tables.
            conn.execute("PRAGMA foreign_keys = ON")

            print("\nValidating required tables...")
            validate_room_carbon_stock_tables(conn)

            print("Checking source data are present...")
            check_room_carbon_source_data_present(conn)

            print("Clearing existing room carbon stock table...")
            clear_room_carbon_stock_table(conn)

            print("Rebuilding room carbon stock...")
            summary = rebuild_room_carbon_stock_table(conn)

            conn.commit()
            return summary

        except Exception:
            # Avoid leaving partially written rows if anything fails mid-build.
            conn.rollback()
            raise

        finally:
            conn.close()

    except DatabaseLockedError:
        raise

    finally:
        # Only release the lock if this script acquired it successfully.
        # If acquire_lock() failed because another user already holds the lock,
        # lock remains None and nothing is released.
        if lock is not None:
            release_lock(lock)


# Public function: validate all sources and targets (fail fast)
def validate_room_carbon_stock_tables(conn: sqlite3.Connection) -> None:
    """
    Check that all required source and target tables exist.

    Required source tables:
        - item_count_summary
        - item_dictionary
        - furniture_class

    Required target tables:
        - room_carbon_stock

    Fail fast here if the DB has not been initialised correctly, or if the
    schema is out-of-date relative to the modelling code.
    """
    required_tables = {
        "item_count_summary",
        "item_dictionary",
        "furniture",
        "room_carbon_stock",
    }

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

    # Validate the upstream item summary table columns.
    # This is important because room carbon stock depends directly on these
    # summary count outputs from the inventory model.
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

    # Validate lookup tables used to convert expected item counts into
    # expected carbon mass.
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

    # Validate the target table contains the expected current columns.
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


# Public function: Check source table not empty
def check_room_carbon_source_data_present(conn: sqlite3.Connection) -> None:
    """
    Check that the source summary / lookup tables contain data.

    This distinguishes:
        - schema exists, but upstream modelling has not yet been run
    from:
        - schema missing entirely

    Current expectation:
        - item_count_summary should contain inventory-derived item count summaries
        - item_dictionary should contain item masses and furniture classes
        - furniture_class should contain carbon factors / split ratios
    """
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM item_count_summary")
    item_summary_rows = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM item_dictionary")
    item_dictionary_rows = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM furniture")
    furniture_class_rows = cur.fetchone()[0]

    if item_summary_rows == 0:
        raise RuntimeError(
            "No source rows found in item_count_summary.\n\n"
            "Run the inventory modelling step first, then rerun the room carbon step."
        )

    if item_dictionary_rows == 0:
        raise RuntimeError(
            "No source rows found in item_dictionary.\n\n"
            "Ingest the vocab/item mapping data first, then rerun the modelling step."
        )

    if furniture_class_rows == 0:
        raise RuntimeError(
            "No source rows found in furniture.\n\n"
            "Ingest the vocab/furniture class data first, then rerun the modelling step."
        )


# Public function: clean target table before rebuild
def clear_room_carbon_stock_table(conn: sqlite3.Connection) -> None:
    """
    Delete existing rows from room_carbon_stock.

    We are deliberately using a simple delete -> rebuild workflow here because:
        - this is an intermediate derived summary table
        - the row count is small
        - full rebuilds are easier to reason about than incremental updates
    """
    cur = conn.cursor()
    cur.execute("DELETE FROM room_carbon_stock")


# Public function
def rebuild_room_carbon_stock_table(conn: sqlite3.Connection) -> dict:
    """
    Rebuild room_carbon_stock from item_count_summary plus lookup tables.

    Grouping level:
        one carbon stock summary row per room_type

    Current room scope:
        - kitchen
        - bedroom
        - living_room
        - unspecified_room

    For each item_name x room_type row in item_count_summary:
        1) read expected_count_mean, count_q25, count_q75
        2) read item_mass from item_dictionary
        3) read kgC_kg, ratio_fossil, ratio_biog from furniture_class
        4) calculate expected total item mass present in the room
        5) convert expected item mass to expected carbon mass
        6) split expected carbon into fossil and biogenic components
        7) sum item contributions across all items within each room_type

    Rows with NULL or non-positive count summary values do not contribute
    to that particular summary metric.
    """
    cur = conn.cursor()

    # Fetch the joined source rows needed for the room-level carbon calculation.
    # Restrict to the agreed first-pass room types only.
    placeholders = ", ".join("?" for _ in sorted(ALLOWED_ROOM_TYPES))

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
            fc.ratio_biog
        FROM item_count_summary AS ics
        JOIN item_dictionary AS id
            ON ics.item_name = id.item_name
        JOIN furniture AS fc
            ON id.furniture_class = fc.furniture_class
        WHERE ics.room_type IN ({placeholders})
        ORDER BY ics.room_type, ics.item_name
    """, tuple(sorted(ALLOWED_ROOM_TYPES)))

    source_rows = cur.fetchall()

    if not source_rows:
        raise RuntimeError(
            "No eligible item_count_summary rows found for the allowed room types:\n"
            + "\n".join(f"  - {room_type}" for room_type in sorted(ALLOWED_ROOM_TYPES))
            + "\n\nRun the upstream inventory model first, or check whether the summary table "
              "contains these room categories."
        )

    # Build room-level accumulators.
    # One accumulator dict per room_type.
    room_totals = {}

    contributing_item_rows = 0

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
        ) = row

        # Fail fast if any required mass/carbon inputs are missing.
        # For this project stage, these should be present in the lookup tables.
        if item_mass is None:
            raise RuntimeError(
                f"NULL item_mass encountered for item_name='{item_name}'."
            )
        if kgC_kg is None:
            raise RuntimeError(
                f"NULL kgC_kg encountered for furniture_class='{furniture_class}'."
            )
        if ratio_fossil is None:
            raise RuntimeError(
                f"NULL ratio_fossil encountered for furniture_class='{furniture_class}'."
            )
        if ratio_biog is None:
            raise RuntimeError(
                f"NULL ratio_biog encountered for furniture_class='{furniture_class}'."
            )

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

        metric_contributed = False

        # ------------------------------------------------------------------
        # Mean-based carbon calculation
        # ------------------------------------------------------------------
        # Step 1:
        # Convert expected item count to expected item mass present in the room:
        #
        #   expected item mass in room
        #       = expected_count_mean * item_mass
        #
        # Step 2:
        # Convert that expected item mass to expected total carbon mass:
        #
        #   expected total carbon
        #       = expected item mass in room * kgC_kg
        #
        # Step 3:
        # Split expected total carbon into fossil and biogenic fractions:
        #
        #   expected fossil carbon = expected total carbon * ratio_fossil
        #   expected biogenic carbon = expected total carbon * ratio_biog
        #
        # These item-level carbon contributions are then added to the running
        # room-level totals for this room_type.
        if expected_count_mean is not None and expected_count_mean > 0:
            expected_item_mass_kg = float(expected_count_mean) * float(item_mass)
            expected_total_carbon_kgC = expected_item_mass_kg * float(kgC_kg)
            expected_fossil_carbon_kgC = expected_total_carbon_kgC * float(ratio_fossil)
            expected_biog_carbon_kgC = expected_total_carbon_kgC * float(ratio_biog)

            room_totals[room_type]["expected_total_carbon_kgC"] += expected_total_carbon_kgC
            room_totals[room_type]["expected_fossil_carbon_kgC"] += expected_fossil_carbon_kgC
            room_totals[room_type]["expected_biog_carbon_kgC"] += expected_biog_carbon_kgC
            metric_contributed = True

        # ------------------------------------------------------------------
        # Q25-based carbon calculation
        # ------------------------------------------------------------------
        # Repeat the same calculation sequence, but using the q25 item count
        # summary from item_count_summary.
        #
        # This gives a lower-count descriptive estimate of room carbon stock.
        if count_q25 is not None and count_q25 > 0:
            q25_item_mass_kg = float(count_q25) * float(item_mass)
            q25_total_carbon_kgC = q25_item_mass_kg * float(kgC_kg)
            q25_fossil_carbon_kgC = q25_total_carbon_kgC * float(ratio_fossil)
            q25_biog_carbon_kgC = q25_total_carbon_kgC * float(ratio_biog)

            room_totals[room_type]["q25_total_carbon_kgC"] += q25_total_carbon_kgC
            room_totals[room_type]["q25_fossil_carbon_kgC"] += q25_fossil_carbon_kgC
            room_totals[room_type]["q25_biog_carbon_kgC"] += q25_biog_carbon_kgC
            metric_contributed = True

        # ------------------------------------------------------------------
        # Q75-based carbon calculation
        # ------------------------------------------------------------------
        # Repeat the same calculation sequence again, now using the q75 item
        # count summary from item_count_summary.
        #
        # This gives a higher-count descriptive estimate of room carbon stock.
        if count_q75 is not None and count_q75 > 0:
            q75_item_mass_kg = float(count_q75) * float(item_mass)
            q75_total_carbon_kgC = q75_item_mass_kg * float(kgC_kg)
            q75_fossil_carbon_kgC = q75_total_carbon_kgC * float(ratio_fossil)
            q75_biog_carbon_kgC = q75_total_carbon_kgC * float(ratio_biog)

            room_totals[room_type]["q75_total_carbon_kgC"] += q75_total_carbon_kgC
            room_totals[room_type]["q75_fossil_carbon_kgC"] += q75_fossil_carbon_kgC
            room_totals[room_type]["q75_biog_carbon_kgC"] += q75_biog_carbon_kgC
            metric_contributed = True

        if metric_contributed:
            contributing_item_rows += 1

    # Insert one summary row per room_type.
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
            None,   # notes intentionally unused for now
        ))
        rows_written += 1

    return {
        "source_rows": len(source_rows),
        "contributing_item_rows": contributing_item_rows,
        "room_rows_written": rows_written,
    }


# Internal helper
def require_columns(
    conn: sqlite3.Connection,
    table_name: str,
    required_columns: set[str],
) -> None:
    """
    Validate that a given table contains the expected columns.

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