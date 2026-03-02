# scripts/ingest_vocab.py
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from scripts import vocab

# Enforce ingestion from only this file (must exist).
EXPECTED_FILENAME = "mapping_list.xlsx"

# Function: check that mapping_list.xlsx exists
def scan_inputs(raw_dir: Path) -> list[Path]:
    """
    Strict scanner for vocab mapping list.

    Returns:
        [raw_dir / "mapping_list.xlsx"] if it exists.

    Raises:
        FileNotFoundError if mapping_list.xlsx is missing.
    """
    raw_dir = Path(raw_dir)
    xlsx_path = raw_dir / EXPECTED_FILENAME
    if not xlsx_path.exists():
        raise FileNotFoundError(
            f"Vocab mapping list not found: {xlsx_path}\n"
            f"Expected exactly one file named '{EXPECTED_FILENAME}' in the raw vocab directory."
        )
    return [xlsx_path]


def _count_rows(db_path: Path) -> dict[str, int]:
    """
    Counts rows in vocab tables.
    Assumes tables exist in the schema; raises sqlite errors if not.
    """
    db_path = Path(db_path)
    con = sqlite3.connect(str(db_path))   # opens SQLite connection to DB
    try:
        con.execute("PRAGMA foreign_keys = ON;")  # Enables foreign key enforcement
        cur = con.cursor() # Creates a cursor to execute queries

        # Private function to count rows in any table
        def _count(table: str) -> int:
            row = cur.execute(f"SELECT COUNT(*) FROM {table};").fetchone()
            return int(row[0]) if row and row[0] is not None else 0

        n_items = _count("item_dictionary")
        n_classes = _count("furniture_class")
        n_rooms = _count("room_type")

        return {
            "rows_item_dictionary": n_items,
            "rows_furniture_class": n_classes,
            "rows_room_type": n_rooms,
            "rows_total": n_items + n_classes + n_rooms,
        }
    finally:
        con.close()

# Private function: Ensures the file exists before planning or ingesting.
def _validate_single_mapping_list(files: list[Path]) -> Path:
    """
    Enforce that vocab ingestion only accepts a single workbook named mapping_list.xlsx.
    """
    if len(files) != 1:
        raise ValueError(
            f"Vocab ingester expects exactly one file: {EXPECTED_FILENAME}. "
            f"Got {len(files)} file(s): {[str(p) for p in files]}"
        )

    p = Path(files[0])

    # Reject non-xlsx.
    if p.suffix.lower() != ".xlsx":   
        raise ValueError(f"Vocab ingester expects an .xlsx file. Got: {p}")

    # Enforces exact filename
    if p.name.lower() != EXPECTED_FILENAME.lower(): 
        raise ValueError(
            f"Vocab ingester only accepts '{EXPECTED_FILENAME}'. Got: {p.name}"
        )

    if not p.exists():
        raise FileNotFoundError(f"Input file does not exist: {p}")

    return p

# Function: Dispatcher “dry-run planning” step
# (uses this to identify new_files)
def plan(db_path: Path, raw_dir: Path, input_files: list[Path]) -> dict[str, Any]:
    """
    Plan vocab ingestion.

    Current policy:
      - treat the mapping_list.xlsx file as 'new' whenever it exists.
      - 'already_ingested' is the current total row count across vocab tables.
    """
    _ = Path(raw_dir)  # kept for consistency with other ingesters (not used here)
    xlsx_path = _validate_single_mapping_list(input_files)

    # Count current vocab table rows (what already exists)
    counts = _count_rows(Path(db_path))
    already = int(counts.get("rows_total", 0))

    # Returns plan dict in the form expected by ingest.py:
    # a list of paths + count value of already ingested rows (int of None)
    return {
        "new": [xlsx_path], 
        "already_ingested": already,
    }

# Function: find obsolete values - not required for vocab! 
def prune_preview(db_path: Path, raw_dir: Path) -> list[Any]:
    """
    No pruning concept for vocab.
    Either rewrite [default] or upsert.

    Required in other ingest_<source> scripts.
    """
    _ = db_path, raw_dir
    return []

# Function: prune obsolete values - not required for vocab! 
def prune_apply(db_path: Path, raw_dir: Path) -> dict[str, Any]:
    """
    No pruning concept for vocab.

    No-op but returns a friendly summary dict
    """
    _ = db_path, raw_dir
    return {"rows_deleted": 0, "note": "not applicable"}

# Function: 
def ingest_apply(db_path: Path, raw_dir: Path, new_files: list[Path]) -> dict[str, Any]:
    """
    Apply vocab ingestion using vocab.ingest_mapping_list_pandas().

    Default mode: replace_all (overwrite existing + add new).
    """
    _ = Path(raw_dir)  # kept for signature consistency; not used currently
    xlsx_path = _validate_single_mapping_list(new_files)

    # Apply ingest (DB write). 
    # Function called from vocab.py.
    vocab.ingest_mapping_list_pandas(
        db_path=Path(db_path),
        xlsx_path=xlsx_path,
        mode="replace_all",
    )

    # Post-ingest counts (nice-to-have)
    counts = _count_rows(Path(db_path))

    return {
        "file": str(xlsx_path),
        "mode": "replace_all",
        **counts,
    }