# scripts/ingest_assumed_items.py
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from scripts import assumed_items


# -------------------------------------
# EXPECTED SOURCE FILE
# Enforce ingestion from only this file
# -------------------------------------

EXPECTED_FILENAME = "assumed_items.xlsx"


# -------------------------------------
# SCAN INPUTS
# Checks that assumed_items.xlsx exists
# -------------------------------------

def scan_inputs(raw_dir: Path) -> list[Path]:
    """
    Strict scanner for assumed inventory assumptions.

    Returns:
        [raw_dir / "assumed_items.xlsx"] if it exists.

    Raises:
        FileNotFoundError if assumed_items.xlsx is missing.
    """

    raw_dir = Path(raw_dir)
    xlsx_path = raw_dir / EXPECTED_FILENAME

    if not xlsx_path.exists():
        raise FileNotFoundError(
            f"Assumed inventory file not found: {xlsx_path}\n"
            f"Expected exactly one file named '{EXPECTED_FILENAME}' "
            "in the assumed inventory raw/config directory."
        )

    return [xlsx_path]


# -------------------------------------
# COUNT EXISTING ROWS
# Used by plan() and ingest_apply()
# -------------------------------------

def _count_rows(db_path: Path) -> dict[str, int]:
    """
    Counts rows in assumed_inventory.

    Assumes the table exists in the schema; raises sqlite errors if not.
    """

    db_path = Path(db_path)

    con = sqlite3.connect(str(db_path))

    try:
        con.execute("PRAGMA foreign_keys = ON;")
        cur = con.cursor()

        row = cur.execute(
            """
            SELECT COUNT(*)
            FROM assumed_inventory;
            """
        ).fetchone()

        n_assumed = int(row[0]) if row and row[0] is not None else 0

        return {
            "rows_assumed_inventory": n_assumed,
            "rows_total": n_assumed,
        }

    finally:
        con.close()


# -------------------------------------
# VALIDATE SINGLE INPUT FILE
# Ensures this ingester only receives assumed_items.xlsx
# -------------------------------------

def _validate_single_assumed_items_file(files: list[Path]) -> Path:
    """
    Enforce that assumed inventory ingestion only accepts a single workbook
    named assumed_items.xlsx.
    """

    if len(files) != 1:
        raise ValueError(
            f"Assumed inventory ingester expects exactly one file: {EXPECTED_FILENAME}. "
            f"Got {len(files)} file(s): {[str(p) for p in files]}"
        )

    p = Path(files[0])

    # Reject non-xlsx files.
    if p.suffix.lower() != ".xlsx":
        raise ValueError(
            f"Assumed inventory ingester expects an .xlsx file. Got: {p}"
        )

    # Enforce exact filename.
    if p.name.lower() != EXPECTED_FILENAME.lower():
        raise ValueError(
            f"Assumed inventory ingester only accepts '{EXPECTED_FILENAME}'. "
            f"Got: {p.name}"
        )

    if not p.exists():
        raise FileNotFoundError(f"Input file does not exist: {p}")

    return p


# -------------------------------------
# PLAN INGEST
# Dispatcher dry-run step
# -------------------------------------

def plan(db_path: Path, raw_dir: Path, input_files: list[Path]) -> dict[str, Any]:
    """
    Plan assumed inventory ingestion.

    Current policy:
      - treat assumed_items.xlsx as 'new' whenever it exists.
      - 'already_ingested' is the current row count in assumed_inventory.

    Notes:
      - This mirrors the vocab ingester pattern.
      - The file is treated as a canonical controlled-config file.
    """

    _ = Path(raw_dir)  # kept for consistency with other ingesters; not used here

    xlsx_path = _validate_single_assumed_items_file(input_files)

    counts = _count_rows(Path(db_path))
    already = int(counts.get("rows_total", 0))

    return {
        "new": [xlsx_path],
        "already_ingested": already,
    }


# -------------------------------------
# PRUNE PREVIEW
# Not required for assumed inventory
# -------------------------------------

def prune_preview(db_path: Path, raw_dir: Path) -> list[Any]:
    """
    No separate pruning concept for assumed inventory.

    Obsolete rows are handled implicitly by ingest_apply() when running in
    replace_all mode.
    """

    _ = db_path, raw_dir
    return []


# -------------------------------------
# PRUNE APPLY
# Not required for assumed inventory
# -------------------------------------

def prune_apply(db_path: Path, raw_dir: Path) -> dict[str, Any]:
    """
    Separate prune apply is not implemented for assumed inventory.

    Obsolete assumed rows are handled implicitly by ingest_apply() when
    running in replace_all mode.
    """

    _ = db_path, raw_dir

    return {
        "rows_deleted": 0,
        "note": "not applicable",
    }


# -------------------------------------
# APPLY INGEST
# Calls assumed_items.py to validate and insert data
# -------------------------------------

def ingest_apply(db_path: Path, raw_dir: Path, new_files: list[Path]) -> dict[str, Any]:
    """
    Apply assumed inventory ingestion using assumed_items.ingest_assumed_items_pandas().

    Default mode:
      replace_all

    This means:
      - validate assumed_items.xlsx
      - delete existing assumed_inventory rows
      - insert all validated rows from the workbook
    """

    _ = Path(raw_dir)  # kept for signature consistency; not used currently

    xlsx_path = _validate_single_assumed_items_file(new_files)

    # Apply ingest.
    # Main validation and DB-writing logic lives in scripts/assumed_items.py.
    assumed_items.ingest_assumed_items_pandas(
        db_path=Path(db_path),
        xlsx_path=xlsx_path,
        mode="replace_all",
    )

    # Post-ingest counts.
    counts = _count_rows(Path(db_path))

    return {
        "file": str(xlsx_path),
        "mode": "replace_all",
        **counts,
    }