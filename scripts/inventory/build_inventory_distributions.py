# scripts/build_inventory_distributions.py
"""
Build survey-derived inventory distribution tables in the shared SQLite database.

This script rebuilds the following intermediate modelling tables from the
observed survey data already stored in the database:

    - item_count_pmf
    - item_count_summary
    - room_count_pmf
    - room_count_summary

Current scope:
    (i) item counts within each room_type, using inventory_observations
    (ii) model-ready room counts within each dwelling, using dwelling_observations

Bedroom-count interpretation:
    - dwelling_observations stores the raw bedroom survey value as the total
      number of bedrooms in the dwelling.
    - In this script, that source value is referred to as total_bedroom_count.
    - The room_count_* modelling outputs partition total_bedroom_count into
      fire-model room types:
          * bedroom          = first / main bedroom, 0 or 1
          * bedroom_second   = second bedroom, 0 or 1
          * bedroom_three_up = third-and-higher bedrooms, 0 or greater

Special room-count cases:
    - unspecified_room is added as a synthetic conditional room-count row,
      because it is a valid generic fire-model room archetype but is not a
      survey room-count option.
    - unknown is intentionally not added here. It is an input-only fire-case
      uncertainty category, not an inventory room archetype.

Important design choices for the current project stage:
    - Uses raw empirical frequencies only (no smoothing / shrinkage yet)
    - Includes zero counts, because these preserve the true observed population
    - Rebuilds target tables from scratch each time (delete -> rebuild)
    - Uses fixed support 0..10 inclusive for both item and room counts,
      because the survey count questions are capped at 10
    - Leaves notes fields as NULL for direct survey-derived rows
    - Adds explanatory notes for derived/synthetic room-count rows
    - Computes summary values:
          * expected_count_mean
          * count_q25
          * count_q75
      where q25 / q75 are interpolated empirical quartiles

This is an intermediate modelling step, not the final fire/emissions calculation step.
The resulting tables are intended to be reused later by downstream modelling scripts.
"""

from __future__ import annotations

import sqlite3
from collections import Counter
from pathlib import Path

from scripts.db_lock import acquire_lock, release_lock, DatabaseLockedError


# Fixed survey support for all count questions.
# We now constrain both item counts and room counts to 0..10 inclusive,
# so the PMF tables should always contain all 11 possible count values.
MIN_COUNT = 0
MAX_COUNT = 10


# The survey uses room_type='bedroom' to store the total number of bedrooms
# in the dwelling. To avoid semantic confusion, the modelling code refers to
# this source value as total_bedroom_count and partitions it into fire-model
# room archetypes before writing room_count_pmf / room_count_summary.
TOTAL_BEDROOM_SOURCE_ROOM_TYPE = "bedroom"


# Fire-model bedroom room types derived from total_bedroom_count.
# These are the room_type values expected by downstream room carbon and fire
# impact modelling code.
BEDROOM_PARTITION_ROOM_TYPES = (
    "bedroom",
    "bedroom_second",
    "bedroom_three_up",
)


# Synthetic conditional room-count rows.
#
# These are model-ready room archetypes that do not correspond to a valid
# survey room-count question. They are only meaningful conditionally: if a
# fire case or scenario chooses this room_type, model one affected room of
# that archetype.
#
# Do not include 'unknown' here. unknown is an input-only uncertainty category,
# not a real inventory room archetype with count or contents.
SYNTHETIC_CONDITIONAL_ROOM_COUNTS = {
    "unspecified_room": 1,
}


# Public function
def build_inventory_distributions(db_path: Path) -> dict:
    """
    Rebuild all inventory-derived count PMF / summary tables.

    Workflow:
        1) Acquire DB lock (shared DB may be accessed by multiple collaborators)
        2) Validate required source / target tables exist
        3) Check source tables contain data
        4) Delete old distribution rows
        5) Rebuild item count PMFs / summaries
        6) Rebuild room count PMFs / summaries
        7) Commit changes and release lock

    Returns a compact summary dict for printing by scripts/model.py.
    """
    lock = None
    
    # Locks the database to prevent accidental simultaneous write (from db_lock.py)
    try:
        lock = acquire_lock(db_path, purpose="build inventory distributions")
        conn = sqlite3.connect(db_path)

        # Validation callers for required tables
        try:
            # Use foreign keys consistently, as target tables reference vocab tables.
            conn.execute("PRAGMA foreign_keys = ON")

            print("\nValidating required tables...")
            validate_inventory_distribution_tables(conn)

            print("Checking source data are present...")
            check_source_data_present(conn)

            print("Clearing existing distribution tables...")
            clear_inventory_distribution_tables(conn)

            print("Rebuilding item count distributions...")
            item_summary = build_item_count_distributions(conn)

            print("Rebuilding room count distributions...")
            room_summary = build_room_count_distributions(conn)

            conn.commit()

            return {
                "item_groups": item_summary["groups"],
                "item_pmf_rows": item_summary["pmf_rows"],
                "item_summary_rows": item_summary["summary_rows"],
                "room_groups": room_summary["groups"],
                "room_pmf_rows": room_summary["pmf_rows"],
                "room_summary_rows": room_summary["summary_rows"],
            }

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
def validate_inventory_distribution_tables(conn: sqlite3.Connection) -> None:
    """
    Check that all required source and target tables exist.

    Required source tables:
        - inventory_observations
        - dwelling_observations

    Required target tables:
        - item_count_pmf
        - item_count_summary
        - room_count_pmf
        - room_count_summary

    Fail fast here if the DB has not been initialised correctly, or if the
    schema is out-of-date relative to the modelling code.
    """
    required_tables = {
        "inventory_observations",
        "dwelling_observations",
        "room",
        "item_count_pmf",
        "item_count_summary",
        "room_count_pmf",
        "room_count_summary",
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

    # Validate both PMF table columns
    require_columns(
        conn,
        table_name="item_count_pmf", 
        required_columns={
            "item_name",
            "room_type",
            "count_value",
            "item_frequency",
            "item_probability",
        },
    )
    require_columns(
        conn,
        table_name="room_count_pmf",
        required_columns={
            "room_type",
            "count_value",
            "room_frequency",
            "room_probability",
        },
    )
    
    # Also check that the summary tables contain the current expected columns.
    require_columns(
        conn,
        table_name="item_count_summary",
        required_columns={"item_name", "room_type", "expected_count_mean", "count_q25", "count_q75"},
    )
    require_columns(
        conn,
        table_name="room_count_summary",
        required_columns={"room_type", "expected_count_mean", "count_q25", "count_q75"},
    )

    # Synthetic conditional room-count rows are checked against the room
    # vocabulary table before insertion. Validate the required vocab column
    # here so stale schemas fail before any modelling writes occur.
    require_columns(
        conn,
        table_name="room",
        required_columns={"room_type"},
    )


# Public function: Check source tables not empty
def check_source_data_present(conn: sqlite3.Connection) -> None:
    """
    Check that the source observation tables contain data.

    This distinguishes:
        - schema exists, but no survey data have been ingested yet
    from:
        - schema missing entirely

    Current expectation:
        - inventory_observations should contain survey item counts
        - dwelling_observations should contain survey room counts
    """
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM inventory_observations")
    inventory_rows = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM dwelling_observations")
    dwelling_rows = cur.fetchone()[0]

    # Errors
    if inventory_rows == 0 and dwelling_rows == 0:
        raise RuntimeError(
            "No source observations found in inventory_observations or dwelling_observations.\n\n"
            "Ingest the survey data first, then rerun the modelling step."
        )

    if inventory_rows == 0:
        raise RuntimeError(
            "No source observations found in inventory_observations.\n\n"
            "Ingest the survey data first, then rerun the modelling step."
        )

    if dwelling_rows == 0:
        raise RuntimeError(
            "No source observations found in dwelling_observations.\n\n"
            "Ingest the survey data first, then rerun the modelling step."
        )


# Public function: clean target tables before rebuild
def clear_inventory_distribution_tables(conn: sqlite3.Connection) -> None:
    """
    Delete existing rows from the four inventory distribution tables.

    We are deliberately using a simple delete -> rebuild workflow here because:
        - survey sample sizes are small
        - the target tables are intermediate summaries, not raw source data
        - full rebuilds are easier to reason about than incremental updates
    """
    cur = conn.cursor()

    # Delete PMF and summary tables
    # Order does not matter since there are no FKs between these target tables themselves.
    cur.execute("DELETE FROM item_count_pmf")
    cur.execute("DELETE FROM item_count_summary")
    cur.execute("DELETE FROM room_count_pmf")
    cur.execute("DELETE FROM room_count_summary")


# Public function: build item count PMF table
def build_item_count_distributions(conn: sqlite3.Connection) -> dict:
    """
    Rebuild item_count_pmf and item_count_summary from inventory_observations.

    Grouping level:
        one empirical distribution per (item_name, room_type)

    For each group:
        - fetch all observed count values (including zeros)
        - build fixed-support PMF over 0..10
        - compute mean / q25 / q75
        - insert 11 PMF rows
        - insert 1 summary row
    """
    cur = conn.cursor()

    # Distinct item-room groups actually present in the observation table.
    # Because zeros are stored, these groups should reflect the true observed
    # population for the survey structure that has been ingested.
    cur.execute("""
        SELECT DISTINCT item_name, room_type
        FROM inventory_observations
        ORDER BY room_type, item_name
    """)
    groups = cur.fetchall()

    pmf_rows_written = 0
    summary_rows_written = 0

    for item_name, room_type in groups:
        cur.execute("""
            SELECT count
            FROM inventory_observations
            WHERE item_name = ?
              AND room_type = ?
            ORDER BY rowid
        """, (item_name, room_type))
        counts = [row[0] for row in cur.fetchall()]

        if not counts:
            # Defensive only - DISTINCT selection above should prevent this.
            continue

        # Convert raw observed counts into:
        #   (i) full PMF rows
        #   (ii) compact summary statistics derived from the PMF rows
        pmf_rows = build_count_pmf(counts, min_count=MIN_COUNT, max_count=MAX_COUNT)
        summary = compute_count_summary_stats(pmf_rows)

        # PMF rows: always 11 rows per item-room group (0..10 inclusive).
        cur.executemany("""
            INSERT INTO item_count_pmf (
                item_name,
                room_type,
                count_value,
                item_frequency,
                item_probability,
                item_pmf_notes
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            (
                item_name,
                room_type,
                row["count_value"],
                row["frequency"],
                row["probability"],
                None,   # notes intentionally unused for now
            )
            for row in pmf_rows
        ])
        pmf_rows_written += len(pmf_rows)

        # Summary row: one per item-room group.
        cur.execute("""
            INSERT INTO item_count_summary (
                item_name,
                room_type,
                expected_count_mean,
                count_q25,
                count_q75,
                count_summary_notes
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            item_name,
            room_type,
            summary["expected_count_mean"],
            summary["count_q25"],
            summary["count_q75"],
            None,   # notes intentionally unused for now
        ))
        summary_rows_written += 1

    return {
        "groups": len(groups),
        "pmf_rows": pmf_rows_written,
        "summary_rows": summary_rows_written,
    }


# Public function
def build_room_count_distributions(conn: sqlite3.Connection) -> dict:
    """
    Rebuild model-ready room_count_pmf and room_count_summary rows.

    Most room types are built directly from dwelling_observations, because the
    observed room count already has the same meaning as the model-ready count.

    Bedroom counts are the important exception:
        - dwelling_observations room_type='bedroom' stores total_bedroom_count
        - the fire model needs separate room archetypes for:
              * bedroom
              * bedroom_second
              * bedroom_three_up

    Therefore this function orchestrates three room-count build paths:
        1) direct survey-derived room types, excluding the raw bedroom source row
        2) partitioned bedroom room types derived from total_bedroom_count
        3) synthetic conditional room types that are valid fire-model room
           archetypes but not survey count options, e.g. unspecified_room

    Returns:
        A compact summary dict matching the previous public interface, with
        additional breakdown fields for debugging / CLI reporting if desired.
    """

    # Direct room types are handled exactly as before, except that the raw
    # bedroom source value is skipped. It is not model-ready as-is because it
    # represents total_bedroom_count, not the first/main bedroom archetype.
    direct_summary = build_direct_room_count_distributions(
        conn,
        excluded_room_types={TOTAL_BEDROOM_SOURCE_ROOM_TYPE},
    )

    # Build bedroom, bedroom_second and bedroom_three_up directly from the raw
    # total_bedroom_count values in dwelling_observations.
    bedroom_partition_summary = partition_bedroom_count_distributions(conn)

    # Add clearly marked synthetic conditional rows such as unspecified_room.
    # These are not survey-derived. They exist so downstream fire-model code can
    # use the archetype without needing a real per-dwelling survey count.
    synthetic_summary = add_synthetic_room_count_distributions(conn)

    return {
        "groups": (
            direct_summary["groups"]
            + bedroom_partition_summary["groups"]
            + synthetic_summary["groups"]
        ),
        "pmf_rows": (
            direct_summary["pmf_rows"]
            + bedroom_partition_summary["pmf_rows"]
            + synthetic_summary["pmf_rows"]
        ),
        "summary_rows": (
            direct_summary["summary_rows"]
            + bedroom_partition_summary["summary_rows"]
            + synthetic_summary["summary_rows"]
        ),
        "direct_groups": direct_summary["groups"],
        "bedroom_partition_groups": bedroom_partition_summary["groups"],
        "synthetic_groups": synthetic_summary["groups"],
    }


# Internal helper
def build_direct_room_count_distributions(
    conn: sqlite3.Connection,
    *,
    excluded_room_types: set[str] | None = None,
) -> dict:
    """
    Build direct survey-derived room-count PMFs / summaries.

    This helper handles room types where the count in dwelling_observations can
    be used directly as the model-ready room count.

    For example:
        living_room count in dwelling_observations
            -> living_room count in room_count_summary

    It deliberately skips room types supplied in excluded_room_types. The main
    current example is room_type='bedroom', because the source bedroom value is
    total_bedroom_count and must be partitioned before it is model-ready.
    """
    if excluded_room_types is None:
        excluded_room_types = set()

    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT room_type
        FROM dwelling_observations
        ORDER BY room_type
    """)
    groups = [
        row[0]
        for row in cur.fetchall()
        if row[0] not in excluded_room_types
    ]

    pmf_rows_written = 0
    summary_rows_written = 0

    for room_type in groups:
        cur.execute("""
            SELECT count
            FROM dwelling_observations
            WHERE room_type = ?
            ORDER BY rowid
        """, (room_type,))
        counts = [row[0] for row in cur.fetchall()]

        if not counts:
            # Defensive only - DISTINCT selection above should prevent this.
            continue

        inserted = insert_room_count_distribution(
            conn,
            room_type=room_type,
            counts=counts,
            pmf_notes=None,
            summary_notes=None,
        )
        pmf_rows_written += inserted["pmf_rows"]
        summary_rows_written += inserted["summary_rows"]

    return {
        "groups": len(groups),
        "pmf_rows": pmf_rows_written,
        "summary_rows": summary_rows_written,
    }


# Internal helper
def partition_bedroom_count_distributions(conn: sqlite3.Connection) -> dict:
    """
    Build bedroom room-count distributions from total_bedroom_count.

    Source:
        dwelling_observations where room_type='bedroom'

    Terminology:
        total_bedroom_count = total number of bedrooms in the dwelling,
        as reported by the survey/source data.

    Derived fire-model room counts:
        bedroom          = min(total_bedroom_count, 1)
        bedroom_second   = 1 if total_bedroom_count >= 2 else 0
        bedroom_three_up = max(total_bedroom_count - 2, 0)

    This strict partition means:
        total_bedroom_count = bedroom + bedroom_second + bedroom_three_up

    Important semantic note:
        The output room_type='bedroom' is the first/main bedroom partition.
        It is not the total bedroom count.
    """
    cur = conn.cursor()

    cur.execute("""
        SELECT count
        FROM dwelling_observations
        WHERE room_type = ?
        ORDER BY rowid
    """, (TOTAL_BEDROOM_SOURCE_ROOM_TYPE,))
    total_bedroom_counts = [row[0] for row in cur.fetchall()]

    if not total_bedroom_counts:
        return {
            "groups": 0,
            "pmf_rows": 0,
            "summary_rows": 0,
        }

    # Partition each observed total_bedroom_count into the three fire-model
    # bedroom categories. Each list below has the same length as the source
    # total_bedroom_counts list, preserving the empirical dwelling sample size.
    partitioned_counts = {
        "bedroom": [
            min(total_bedroom_count, 1)
            for total_bedroom_count in total_bedroom_counts
        ],
        "bedroom_second": [
            1 if total_bedroom_count >= 2 else 0
            for total_bedroom_count in total_bedroom_counts
        ],
        "bedroom_three_up": [
            max(total_bedroom_count - 2, 0)
            for total_bedroom_count in total_bedroom_counts
        ],
    }

    pmf_notes = (
        "Derived from total_bedroom_count; bedroom categories are partitioned "
        "fire-model room types."
    )
    summary_notes = (
        "Derived from total_bedroom_count; room_type='bedroom' is the "
        "first/main bedroom partition, not the total bedroom count."
    )

    pmf_rows_written = 0
    summary_rows_written = 0

    for room_type, counts in partitioned_counts.items():
        inserted = insert_room_count_distribution(
            conn,
            room_type=room_type,
            counts=counts,
            pmf_notes=pmf_notes,
            summary_notes=summary_notes,
        )
        pmf_rows_written += inserted["pmf_rows"]
        summary_rows_written += inserted["summary_rows"]

    return {
        "groups": len(partitioned_counts),
        "pmf_rows": pmf_rows_written,
        "summary_rows": summary_rows_written,
    }


# Internal helper
def add_synthetic_room_count_distributions(conn: sqlite3.Connection) -> dict:
    """
    Add synthetic conditional room-count distributions for special room types.

    These rows are not survey-derived. They are used for valid fire-model room
    archetypes that do not correspond to a survey dwelling-count question.

    Current case:
        unspecified_room

    Interpretation:
        If a fire case uses room_type='unspecified_room', model one generic
        affected room of that archetype.

    Important:
        unknown is intentionally not inserted here. It is an input-only fire
        uncertainty category and should be handled as a special case by the
        fire input/snapshot logic, not by assigning it inventory contents.
    """
    pmf_rows_written = 0
    summary_rows_written = 0
    groups_written = 0

    for room_type, fixed_count in SYNTHETIC_CONDITIONAL_ROOM_COUNTS.items():
        if not room_type_exists(conn, room_type):
            # Keep this as a hard failure rather than silently skipping.
            # If code says a synthetic model room should exist, the vocab should
            # contain the matching room_type row.
            raise RuntimeError(
                "Synthetic room-count distribution requested for room_type "
                f"'{room_type}', but this room_type is missing from the room table."
            )

        if room_count_summary_exists(conn, room_type):
            # Defensive only. Under the normal rebuild workflow the table has
            # just been cleared, but this avoids duplicate rows if the helper is
            # ever reused independently.
            continue

        notes = (
            "Synthetic conditional count for generic fire-modelling room "
            "archetype; not survey-derived."
        )

        inserted = insert_room_count_distribution(
            conn,
            room_type=room_type,
            counts=[fixed_count],
            pmf_notes=notes,
            summary_notes=notes,
            # Because this is a fixed synthetic count, keep the compact summary
            # fixed as well. The standard empirical interpolated-quantile helper
            # is useful for survey PMFs, but would otherwise return 0.75 / 1.25
            # for a deterministic count of 1 due to within-bin interpolation.
            summary_override={
                "expected_count_mean": float(fixed_count),
                "count_q25": float(fixed_count),
                "count_q75": float(fixed_count),
            },
        )
        pmf_rows_written += inserted["pmf_rows"]
        summary_rows_written += inserted["summary_rows"]
        groups_written += inserted["groups"]

    return {
        "groups": groups_written,
        "pmf_rows": pmf_rows_written,
        "summary_rows": summary_rows_written,
    }


# Internal helper
def insert_room_count_distribution(
    conn: sqlite3.Connection,
    *,
    room_type: str,
    counts: list[int],
    pmf_notes: str | None = None,
    summary_notes: str | None = None,
    summary_override: dict | None = None,
) -> dict:
    """
    Insert room_count_pmf and room_count_summary rows for one room_type.

    This helper centralises the repeated logic used by:
        - direct survey-derived room counts
        - partitioned bedroom room counts
        - synthetic conditional room counts

    Inputs:
        room_type:
            Target model-ready room_type to write.

        counts:
            Prepared count values for this room_type. These may be raw survey
            values, derived partition values, or synthetic fixed counts.

        pmf_notes / summary_notes:
            Optional explanatory notes written to the target tables. Direct
            survey-derived rows usually leave these as NULL, while derived and
            synthetic rows should explain their interpretation.

        summary_override:
            Optional explicit summary values. This should only be used for
            special synthetic rows where a fixed conditional count should have
            exact q25/q75 values, rather than interpolated empirical quantiles.
    """
    if not counts:
        raise ValueError(
            f"Cannot insert room-count distribution for room_type='{room_type}' "
            "from an empty count list."
        )

    cur = conn.cursor()

    # Convert prepared counts into the standard fixed-support PMF. This keeps
    # the table shape consistent for all room types: 11 rows spanning 0..10.
    pmf_rows = build_count_pmf(counts, min_count=MIN_COUNT, max_count=MAX_COUNT)

    if summary_override is None:
        summary = compute_count_summary_stats(pmf_rows)
    else:
        required_summary_keys = {
            "expected_count_mean",
            "count_q25",
            "count_q75",
        }
        missing = sorted(required_summary_keys - set(summary_override))
        if missing:
            raise ValueError(
                "summary_override is missing required key(s): "
                + ", ".join(missing)
            )
        summary = summary_override

    cur.executemany("""
        INSERT INTO room_count_pmf (
            room_type,
            count_value,
            room_frequency,
            room_probability,
            room_pmf_notes
        )
        VALUES (?, ?, ?, ?, ?)
    """, [
        (
            room_type,
            row["count_value"],
            row["frequency"],
            row["probability"],
            pmf_notes,
        )
        for row in pmf_rows
    ])

    cur.execute("""
        INSERT INTO room_count_summary (
            room_type,
            expected_count_mean,
            count_q25,
            count_q75,
            count_summary_notes
        )
        VALUES (?, ?, ?, ?, ?)
    """, (
        room_type,
        summary["expected_count_mean"],
        summary["count_q25"],
        summary["count_q75"],
        summary_notes,
    ))

    return {
        "groups": 1,
        "pmf_rows": len(pmf_rows),
        "summary_rows": 1,
    }


# Internal helper
def room_type_exists(conn: sqlite3.Connection, room_type: str) -> bool:
    """
    Return True if room_type exists in the room vocabulary table.

    This is used before adding synthetic room-count rows, because those rows do
    not come from dwelling_observations and therefore need an explicit vocab
    sanity check before insertion.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT 1
        FROM room
        WHERE room_type = ?
        LIMIT 1
    """, (room_type,))
    return cur.fetchone() is not None


# Internal helper
def room_count_summary_exists(conn: sqlite3.Connection, room_type: str) -> bool:
    """
    Return True if room_count_summary already has a row for room_type.

    Under the normal full rebuild workflow this should be False for synthetic
    rows, because clear_inventory_distribution_tables() has just run. The check
    is included to make the helper safer if reused in isolation later.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT 1
        FROM room_count_summary
        WHERE room_type = ?
        LIMIT 1
    """, (room_type,))
    return cur.fetchone() is not None


# Public function: carry out PMF
def build_count_pmf(
    counts: list[int],
    min_count: int = 0,
    max_count: int = 10,
) -> list[dict]:
    """
    Build a fixed-support empirical PMF from observed counts.

    Output rows always span the full support min_count..max_count inclusive,
    even where a given count value was never observed. This is important because:
        - survey counts are bounded
        - downstream code can assume a consistent support
        - zero-frequency bins still carry information

    Returns one dict per count value with keys:
        - count_value
        - frequency
        - probability
    """
    if not counts:
        raise ValueError("Cannot build PMF from an empty count list.")

    invalid = [c for c in counts if c < min_count or c > max_count]
    if invalid:
        raise ValueError(
            f"Observed count(s) fall outside permitted support {min_count}..{max_count}: "
            f"{sorted(set(invalid))}"
        )

    n = len(counts)
    freq = Counter(counts)

    pmf_rows = []
    for count_value in range(min_count, max_count + 1):
        # Frequency = number of observations equal to this count value.
        frequency = freq.get(count_value, 0)

        # Probability = empirical relative frequency for this count value.
        probability = frequency / n

        pmf_rows.append({
            "count_value": count_value,
            "frequency": frequency,
            "probability": probability,
        })

    return pmf_rows


# Public function
def compute_count_summary_stats(pmf_rows: list[dict]) -> dict:
    """
    Compute the compact summary statistics stored alongside the PMF.

    Current outputs:
        - expected_count_mean
        - count_q25
        - count_q75

    Important design choice:
        These summary values are derived from the PMF representation,
        rather than directly from the raw count list.

    This keeps the workflow internally consistent:
        raw observed counts -> PMF rows -> summary stats from PMF

    Interpretation:
        - expected_count_mean is the PMF-weighted expected count
        - count_q25 / count_q75 are interpolated quantiles derived from the PMF CDF
    """
    if not pmf_rows:
        raise ValueError("Cannot compute summary statistics from empty PMF rows.")

    # Mean expected count from the PMF.
    # This is the standard discrete expectation:
    #     E[X] = sum( count_value * probability )
    expected_count_mean = sum(
        row["count_value"] * row["probability"]
        for row in pmf_rows
    )

    # Lower and upper quartile bounds from the PMF cumulative distribution.
    # These are calculated from the PMF representation in the database.
    count_q25 = interpolate_quantile_from_pmf(pmf_rows, q=0.25)
    count_q75 = interpolate_quantile_from_pmf(pmf_rows, q=0.75)

    return {
        "expected_count_mean": expected_count_mean,
        "count_q25": count_q25,
        "count_q75": count_q75,
    }


# Internal helper
def interpolate_quantile_from_pmf(pmf_rows: list[dict], q: float) -> float:
    """
    Compute an interpolated quantile directly from the PMF rows.

    Inputs:
        pmf_rows : list of dicts with keys:
            - count_value
            - probability
        q : target quantile in [0, 1]

    Method:
        1) Build the cumulative probability distribution (CDF)
        2) Find the first PMF bin whose cumulative probability reaches/exceeds q
        3) Interpolate within that bin, treating the probability mass as spread
           uniformly across the width of that discrete count interval

    Why use this approach:
        - keeps summary statistics tied directly to the PMF representation
        - allows float-valued quartile outputs
          (otherwise quartiles would primarily produce 0 or 1) 
        - gives a smoother spread summary than returning only integer count values

    Interpretation note:
        The interpolated quartile is not a literally observed count.
        It is a descriptive position within the PMF/CDF,
        intended to accompany the expected count mean in the summary table.
    """
    if not pmf_rows:
        raise ValueError("Cannot compute quantile from empty PMF rows.")
    if q < 0.0 or q > 1.0:
        raise ValueError(f"Quantile q must lie in [0, 1]. Got: {q}")

    cumulative = 0.0

    for i, row in enumerate(pmf_rows):
        count_value = float(row["count_value"])
        probability = float(row["probability"])

        prev_cumulative = cumulative
        cumulative += probability

        # Skip bins with zero probability - they do not occupy any CDF width.
        if probability <= 0.0:
            continue

        # This is the first bin whose cumulative probability reaches the target q.
        if q <= cumulative:
            # Fraction of the way through this PMF bin needed to reach q.
            # For example:
            #   if previous cumulative probability = 0.20
            #   and current cumulative probability  = 0.40
            #   then q = 0.25 lies one-quarter of the way through this bin.
            frac = (q - prev_cumulative) / probability

            # Interpolate within the current count bin.
            #
            # We treat each discrete count as occupying the interval:
            #   [count_value - 0.5, count_value + 0.5]
            #
            # This gives a continuous descriptive summary from a discrete PMF.
            lower_edge = count_value - 0.5
            upper_edge = count_value + 0.5

            quantile_value = lower_edge + frac * (upper_edge - lower_edge)

            # Keep results within the overall bounded survey support.
            min_supported = float(pmf_rows[0]["count_value"])
            max_supported = float(pmf_rows[-1]["count_value"])
            return max(min_supported, min(max_supported, quantile_value))

    # Fallback for any floating-point edge case at q = 1.0.
    return float(pmf_rows[-1]["count_value"])


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