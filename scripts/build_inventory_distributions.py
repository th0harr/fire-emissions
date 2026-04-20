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
    (ii) room counts within each dwelling, using dwelling_observations

Important design choices for the current project stage:
    - Uses raw empirical frequencies only (no smoothing / shrinkage yet)
    - Includes zero counts, because these preserve the true observed population
    - Rebuilds target tables from scratch each time (delete -> rebuild)
    - Uses fixed support 0..10 inclusive for both item and room counts,
      because the survey count questions are capped at 10
    - Leaves notes fields as NULL for now
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
    # (helps catch stale schema versions before row insertion).
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
    # This is particularly useful while the schema is still evolving
    # (e.g. new or renamed columns).
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
    # Oder doesn't matter since there are no FKs between these target tables themselves.
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
    Rebuild room_count_pmf and room_count_summary from dwelling_observations.

    Grouping level:
        one empirical distribution per room_type

    Interpretation:
        this estimates the empirical distribution of how many rooms of a given
        type occur within a dwelling (e.g. bedrooms per dwelling, bathrooms per dwelling).

    For each group:
        - fetch all observed count values (including zeros)
        - build fixed-support PMF over 0..10
        - compute mean / q25 / q75
        - insert 11 PMF rows
        - insert 1 summary row
    """
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT room_type
        FROM dwelling_observations
        ORDER BY room_type
    """)
    groups = [row[0] for row in cur.fetchall()]

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

        # Convert raw observed counts into:
        #   (i) full fixed-support PMF rows
        #   (ii) compact summary statistics derived from those PMF rows
        pmf_rows = build_count_pmf(counts, min_count=MIN_COUNT, max_count=MAX_COUNT)
        summary = compute_count_summary_stats(pmf_rows)

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
                None,   # notes intentionally unused for now
            )
            for row in pmf_rows
        ])
        pmf_rows_written += len(pmf_rows)

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
            None,   # notes intentionally unused for now
        ))
        summary_rows_written += 1

    return {
        "groups": len(groups),
        "pmf_rows": pmf_rows_written,
        "summary_rows": summary_rows_written,
    }


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