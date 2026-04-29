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
def build_room_carbon_stock(
    db_path: Path,
    *,
    assumed: str = "include",
) -> dict:
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
    
    # Validate user-facing assumed inventory option passed from model.py.
    # Current options:
    #   include -> add assumed_inventory contributions into room_carbon_stock
    #   exclude -> build room_carbon_stock from observed/survey-derived items only
    if assumed not in {"include", "exclude"}:
        raise ValueError("assumed must be either 'include' or 'exclude'")
    
    # Lock the database to prevent accidental simultaneous write (from db_lock.py)
    lock = None
    
    try:
        lock = acquire_lock(db_path, purpose="build room carbon stock")
        conn = sqlite3.connect(db_path)

        try:
            # Use foreign keys consistently, as target tables reference vocab tables.
            conn.execute("PRAGMA foreign_keys = ON")

            print("\nValidating required tables...")
            validate_room_carbon_stock_tables(conn, assumed=assumed)

            print("Checking source data are present...")
            check_room_carbon_source_data_present(conn, assumed=assumed)

            print("Clearing existing room carbon stock table...")
            clear_room_carbon_stock_table(conn)

            print("Rebuilding room carbon stock...")
            summary = rebuild_room_carbon_stock_table(conn, assumed=assumed)

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
def validate_room_carbon_stock_tables(
    conn: sqlite3.Connection,
    *,
    assumed: str = "include",
) -> None:
    """
    Check that all required source and target tables exist.

    Required source tables:
        - item_count_summary
        - item_dictionary
        - furniture_class
        - assumed_inventory (unless excluded)

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

    # Assumed inventory is optional from the CLI perspective.
    # If the user runs:
    #   --assumed include
    # then the model requires assumed_inventory and room_count_summary.
    #
    # room_count_summary is needed because dependency_type='room_type'
    # uses mean/q25/q75 room counts as dependency multipliers.
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

    # Validate assumed inventory inputs only when assumed items are included.
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


# Public function: Check source table not empty
def check_room_carbon_source_data_present(
    conn: sqlite3.Connection,
    *,
    assumed: str = "include",
) -> None:
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
        - assumed_inventory should contain items, counts and room types
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

    # Assumed inventory is only required when explicitly included.
    # (Although it is included by default)
    # This allows sensitivity tests using:
    #   --assumed exclude
    if assumed == "include":
        cur.execute("SELECT COUNT(*) FROM assumed_inventory")
        assumed_inventory_rows = cur.fetchone()[0]

        if assumed_inventory_rows == 0:
            raise RuntimeError(
                "No source rows found in assumed_inventory.\n\n"
                "Run the assumed items ingester first, or rerun room_carbon with:\n"
                "  --assumed exclude"
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



# Internal helper
def ensure_room_total_accumulator(room_totals: dict, room_type: str) -> None:
    """
    Ensure room_totals contains an accumulator dictionary for room_type.

    This is shared by:
        - observed/survey-derived item count rows
        - assumed_inventory item rows

    Keeping this in one helper avoids duplicating the same accumulator setup
    in multiple modelling branches.
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


# Internal helper function
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
    Add one item category's carbon contribution to the room-level accumulator.

    The supplied count values may represent:
    - an empirical mean/q25/q75 count from item_count_summary
    - an assumed effective mean/q25/q75 count from assumed_inventory

    This helper performs the shared carbon calculation used for both:
        1) observed/survey-derived item_count_summary rows
        2) assumed_inventory rows

    Calculation:
        item mass in room = item count * item_mass
        total carbon      = item mass in room * kgC_kg
        fossil carbon     = total carbon * ratio_fossil
        biogenic carbon   = total carbon * ratio_biog

    The calculation is repeated independently for:
        - mean count
        - q25 count
        - q75 count

    Returns:
        True if at least one of mean/q25/q75 contributed a positive value.
        False otherwise.
    """
    ensure_room_total_accumulator(room_totals, room_type)

    metric_contributed = False

    # ------------------------------------------------------------------
    # Mean-based carbon calculation
    # ------------------------------------------------------------------
    if count_mean is not None and count_mean > 0:
        expected_item_mass_kg = float(count_mean) * float(item_mass)
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


# Internal helper function
def add_assumed_inventory_to_room_totals(
    conn: sqlite3.Connection,
    room_totals: dict,
) -> dict:
    """
    Add assumed_inventory item carbon contributions into room_totals.

    Important interpretation:
        assumed_inventory rows represent curated model assumptions, not
        empirical survey responses.

    For each assumed item:
        effective mean count =
            count_assumed + dependency_quantifier * dependency mean count

        effective q25 count =
            count_assumed + dependency_quantifier * dependency q25 count

        effective q75 count =
            count_assumed + dependency_quantifier * dependency q75 count

    Where dependency counts are read from:
        - item_count_summary, when dependency_type='item_name'
        - room_count_summary, when dependency_type='room_type'

    For dependency_type='item_name':
        the dependency item is looked up in the same room_type as the assumed item.

        Example:
            food in kitchen depends on kitchen_cupboard in kitchen.

    Carbon conversion uses the assumed item's own:
        - item_mass
        - kgC_kg
        - ratio_fossil
        - ratio_biog

    It does NOT use the dependency item's mass/material properties
    (e.g. it uses mass of food not the mass of kitchen_cupboard).
    """
    cur = conn.cursor()

    cur.execute("""
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
            fc.ratio_biog
        FROM assumed_inventory AS ai
        JOIN item_dictionary AS id
            ON ai.item_name = id.item_name
        JOIN furniture AS fc
            ON id.furniture_class = fc.furniture_class
        ORDER BY ai.room_type, ai.item_name
    """)

    assumed_rows = cur.fetchall()

    assumed_rows_contributing = 0

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
        ) = row

        # Fail fast if any required mass/carbon inputs are missing.
        # These should already be controlled by vocab ingest, but this gives
        # a clearer modelling-stage error if the database is inconsistent.
        if item_mass is None:
            raise RuntimeError(
                f"NULL item_mass encountered for assumed item_name='{item_name}'."
            )
        if kgC_kg is None:
            raise RuntimeError(
                f"NULL kgC_kg encountered for assumed furniture_class='{furniture_class}'."
            )
        if ratio_fossil is None:
            raise RuntimeError(
                f"NULL ratio_fossil encountered for assumed furniture_class='{furniture_class}'."
            )
        if ratio_biog is None:
            raise RuntimeError(
                f"NULL ratio_biog encountered for assumed furniture_class='{furniture_class}'."
            )

        # Start with the always-present assumed count.
        count_mean = float(count_assumed)
        count_q25 = float(count_assumed)
        count_q75 = float(count_assumed)

        # --------------------------------------------------------------
        # Dependency case 1:
        # Assumed item count depends on the mean/q25/q75 count of another
        # item_name in the same room_type.
        # --------------------------------------------------------------
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
                    "Assumed inventory dependency could not be resolved in item_count_summary:\n"
                    f"  assumed item: {item_name}\n"
                    f"  assumed room: {room_type}\n"
                    f"  dependency item_name: {dependency}\n\n"
                    "Run the inventory model first, or check that the dependency item appears "
                    "in item_count_summary for the same room_type."
                )

            dep_mean, dep_q25, dep_q75 = dep_row

            if dep_mean is not None:
                count_mean += float(dependency_quantifier) * float(dep_mean)
            if dep_q25 is not None:
                count_q25 += float(dependency_quantifier) * float(dep_q25)
            if dep_q75 is not None:
                count_q75 += float(dependency_quantifier) * float(dep_q75)

        # --------------------------------------------------------------
        # Dependency case 2:
        # Assumed item count depends on the mean/q25/q75 count of a room_type
        # within the dwelling.
        # --------------------------------------------------------------
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
                    "Assumed inventory dependency could not be resolved in room_count_summary:\n"
                    f"  assumed item: {item_name}\n"
                    f"  assumed room: {room_type}\n"
                    f"  dependency room_type: {dependency}\n\n"
                    "Run the inventory model first, or check that the dependency room_type appears "
                    "in room_count_summary."
                )

            dep_mean, dep_q25, dep_q75 = dep_row

            if dep_mean is not None:
                count_mean += float(dependency_quantifier) * float(dep_mean)
            if dep_q25 is not None:
                count_q25 += float(dependency_quantifier) * float(dep_q25)
            if dep_q75 is not None:
                count_q75 += float(dependency_quantifier) * float(dep_q75)

        # --------------------------------------------------------------
        # Dependency case 3:
        # No dependency fields populated.
        # The assumed item contributes count_assumed only.
        # --------------------------------------------------------------
        elif dependency_type is None:
            pass

        else:
            # This should be impossible if assumed_items ingest validation worked,
            # but keeping this defensive check makes the modelling failure clearer.
            raise RuntimeError(
                f"Unexpected dependency_type='{dependency_type}' for assumed item_name='{item_name}'."
            )

        contributed = add_item_carbon_to_room_totals(
            room_totals=room_totals,
            room_type=room_type,
            count_mean=count_mean,
            count_q25=count_q25,
            count_q75=count_q75,
            item_mass=item_mass,
            kgC_kg=kgC_kg,
            ratio_fossil=ratio_fossil,
            ratio_biog=ratio_biog,
        )

        if contributed:
            assumed_rows_contributing += 1

    return {
        "assumed_rows": len(assumed_rows),
        "assumed_rows_contributing": assumed_rows_contributing,
    }


# Public function
def rebuild_room_carbon_stock_table(
    conn: sqlite3.Connection,
    *,
    assumed: str = "include",
) -> dict:
    """
    Rebuild room_carbon_stock from item_count_summary plus lookup tables.

    Grouping level:
        one carbon stock summary row per room_type

    Current room scope:
        - kitchen
        - bedroom
        - living_room
        - unspecified_room

    Main observed/survey-derived source:
        item_count_summary

    Optional assumed inventory source:
        assumed_inventory

    For each observed item_name x room_type row in item_count_summary:
        1) read expected_count_mean, count_q25, count_q75
        2) read item_mass from item_dictionary
        3) read kgC_kg, ratio_fossil, ratio_biog from furniture
        4) calculate expected item mass present in the room
        5) convert expected item mass to expected carbon mass
        6) split expected carbon into fossil and biogenic components
        7) add item-category contribution to the running room_type totals

    If assumed == "include":
        assumed_inventory rows are added after the observed/survey-derived
        rows have been accumulated, but before final room_carbon_stock rows
        are inserted.

    For assumed_inventory rows:
        - count_assumed is treated as a fixed assumed count
        - dependency fields, where present, modify the effective assumed count
        - dependency_type='item_name' uses item_count_summary for the same room_type
        - dependency_type='room_type' uses room_count_summary
        - carbon conversion uses the assumed item's own item_mass and furniture class

    Rows with NULL or non-positive count summary values do not contribute
    to that particular summary metric.

    Important interpretation note:
        The q25 and q75 room-level carbon values produced here are built by
        summing item-level q25 / q75-derived carbon estimates across the room.

        They are therefore compact descriptive room summaries. They are not yet
        full joint room-level quantiles from a Monte Carlo simulation of room
        contents.
    """

    cur = conn.cursor()

    # Fetch the joined observed/survey-derived source rows needed for the
    # room-level carbon calculation.
    #
    # Each row represents one item category within one room_type, with three
    # count metrics:
    #   - expected_count_mean
    #   - count_q25
    #   - count_q75
    #
    # The item count metrics are joined to item_dictionary and furniture so
    # that the same row also contains the material/carbon parameters needed
    # to convert item counts into carbon mass.
    #
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
    #
    # The dictionary has one nested accumulator per room_type.
    # Each accumulator stores running carbon totals for:
    #   - mean expected count
    #   - q25 count
    #   - q75 count
    #
    # The same accumulator is used for both:
    #   - observed/survey-derived item_count_summary contributions
    #   - optional assumed_inventory contributions
    room_totals = {}

    contributing_item_rows = 0

    # -------------------------------------
    # Add observed/survey-derived item contributions
    # -------------------------------------

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

        # Add this item category's mean/q25/q75 count contribution to the
        # room-level carbon accumulator.
        #
        # The helper performs the shared arithmetic:
        #   count * item_mass * kgC_kg
        #
        # and then splits the total carbon into fossil and biogenic fractions.
        contributed = add_item_carbon_to_room_totals(
            room_totals=room_totals,
            room_type=room_type,
            count_mean=expected_count_mean,
            count_q25=count_q25,
            count_q75=count_q75,
            item_mass=item_mass,
            kgC_kg=kgC_kg,
            ratio_fossil=ratio_fossil,
            ratio_biog=ratio_biog,
        )

        if contributed:
            contributing_item_rows += 1

    # -------------------------------------
    # Optionally add assumed inventory contributions
    # -------------------------------------
    #
    # This is done after the observed/survey-derived item summaries have been
    # accumulated, but before the final room_carbon_stock rows are inserted.
    #
    # This allows sensitivity testing from the CLI:
    #
    #   python -m scripts.model ... --type room_carbon --assumed include
    #   python -m scripts.model ... --type room_carbon --assumed exclude
    #
    # Current default is expected to be:
    #   --assumed include
    #
    # The assumed item calculation is deliberately kept in a separate helper
    # so the core rebuild function remains readable.
    assumed_summary = {
        "assumed_rows": 0,
        "assumed_rows_contributing": 0,
    }

    if assumed == "include":
        assumed_summary = add_assumed_inventory_to_room_totals(
            conn,
            room_totals,
        )

    # -------------------------------------
    # Insert final room-level carbon stock rows
    # -------------------------------------
    #
    # At this point, room_totals contains the final accumulated carbon values
    # for each room_type, either:
    #   - observed items only
    #   - observed items + assumed items
    #
    # depending on the --assumed CLI option.
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
                "Includes assumed_inventory contributions."
                if assumed == "include"
                else "Excludes assumed_inventory contributions."
            ),
        ))

        rows_written += 1

    return {
        "source_rows": len(source_rows),
        "contributing_item_rows": contributing_item_rows,
        "assumed_inventory": assumed,
        "assumed_rows": assumed_summary["assumed_rows"],
        "assumed_rows_contributing": assumed_summary["assumed_rows_contributing"],
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