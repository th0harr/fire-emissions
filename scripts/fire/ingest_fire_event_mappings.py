from __future__ import annotations

"""
Thin ingest-dispatcher wrapper for fire_event_mappings.xlsm.

This mirrors scripts/inventory/ingest_vocab.py:
    - scan_inputs() enforces one controlled workbook
    - plan() performs read-only validation/counting
    - ingest_apply() delegates the real work to scripts.fire.fire_event_mappings
"""

import sqlite3
from pathlib import Path
from typing import Any

from scripts.fire import fire_event_mappings

EXPECTED_FILENAME = "fire_event_mappings.xlsm"


def scan_inputs(raw_dir: str | Path) -> list[Path]:
    """Strict scanner for the fire event mapping workbook."""
    raw_dir = Path(raw_dir)
    xlsx_path = raw_dir / EXPECTED_FILENAME
    if not xlsx_path.exists():
        raise FileNotFoundError(
            f"Fire event mapping workbook not found: {xlsx_path}\n"
            f"Expected exactly one file named '{EXPECTED_FILENAME}' in this raw/config directory."
        )
    return [xlsx_path]


def _validate_single_mapping_file(input_files: list[str | Path]) -> Path:
    """Enforce that ingestion receives exactly one fire_event_mappings.xlsm file."""
    if len(input_files) != 1:
        raise ValueError(
            f"Fire event mapping ingester expects exactly one file: {EXPECTED_FILENAME}. "
            f"Got {len(input_files)} file(s): {[str(p) for p in input_files]}"
        )

    p = Path(input_files[0])

    if p.name.lower() != EXPECTED_FILENAME.lower():
        raise ValueError(
            f"Fire event mapping ingester only accepts '{EXPECTED_FILENAME}'. Got: {p.name}"
        )

    if p.suffix.lower() != ".xlsm":
        raise ValueError(f"Fire event mapping ingester expects an .xlsm file. Got: {p}")

    if not p.exists():
        raise FileNotFoundError(f"Input file does not exist: {p}")

    return p


def _count_rows(db_path: str | Path) -> dict[str, int]:
    """Count rows across all fire_event_mapping_* tables."""
    con = sqlite3.connect(str(db_path))
    try:
        return fire_event_mappings.count_mapping_rows(con)
    finally:
        con.close()


def plan(db_path: str | Path, raw_dir: str | Path, input_files: list[str | Path]) -> dict[str, Any]:
    """Plan fire event mapping ingestion without writing to the database."""
    _ = Path(raw_dir)
    xlsx_path = _validate_single_mapping_file(input_files)

    mapping_plan = fire_event_mappings.build_plan(Path(db_path), xlsx_path)
    counts = _count_rows(Path(db_path))

    return {
        "new": [xlsx_path],
        "already_ingested": counts["rows_total"],
        "details": [fire_event_mappings.summarise_plan(mapping_plan)],
    }


def prune_preview(db_path: str | Path, raw_dir: str | Path) -> list[Any]:
    """No prune concept for fire event mapping config."""
    _ = db_path, raw_dir
    return []


def prune_apply(db_path: str | Path, raw_dir: str | Path) -> dict[str, Any]:
    """Separate prune apply is not implemented for fire event mapping config."""
    _ = db_path, raw_dir
    return {"rows_deleted": 0, "note": "not applicable"}


def ingest_apply(db_path: str | Path, raw_dir: str | Path, new_files: list[str | Path]) -> dict[str, Any]:
    """Apply fire event mapping ingestion in replace-all mode."""
    _ = Path(raw_dir)
    xlsx_path = _validate_single_mapping_file(new_files)

    return fire_event_mappings.ingest_fire_event_mappings(
        db_path=Path(db_path),
        xlsx_path=xlsx_path,
        mode="replace_all",
    )
