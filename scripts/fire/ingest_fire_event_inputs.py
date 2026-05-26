"""
Ingest the fire input parameter workbook into fire_db/test_db.

This ingester reads the controlled fire input workbook:

    fire_input_param.xlsm

and stages:

    1. raw fire input parameters from the `inputs` sheet
    2. fire input value mappings from the `input_mapping` sheet
    3. ignition-source-to-item mappings from the `item_mapping` sheet

Run from the project root through the shared ingest dispatcher, e.g.:

    # Dry run
    python -m scripts.ingest --profile tom --db test_db --type fire_event --scan

    # Apply
    python -m scripts.ingest --profile tom --db test_db --type fire_event --scan --apply

Notes
-----
This is a raw/staging ingest.

It does NOT calculate fire impacts.
It does NOT create final model-facing fire_event_input rows.
That later step should combine these staged rows with the inventory snapshot tables.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from scripts.ingest_utils import (
    IngestLogEntry,
    db_connect,
    record_ingest_run,
    utc_now_iso,
)


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

EXPECTED_FILENAME = "fire_input_param.xlsm"

SOURCE_TYPE = "fire_event"
SOURCE_DESCRIPTION = "Fire event parameter input workbook"
SOURCE_ORG = "internal"

# Workbook sheets
SHEET_INPUTS = "inputs"
SHEET_INPUT_MAPPING = "input_mapping"
SHEET_ITEM_MAPPING = "item_mapping"

# Destination tables
TABLE_SOURCES = "sources"
TABLE_EVENT_PARAMS = "fire_event_parameter_input"
TABLE_INPUT_MAPPING = "fire_input_value_mapping"
TABLE_ITEM_MAPPING = "fire_ignition_item_mapping"

# Inventory snapshot tables used only for validation.
TABLE_INVENTORY_SNAPSHOT = "inventory_snapshot"
TABLE_INVENTORY_ITEMS = "inventory_item_snapshot"
TABLE_INVENTORY_ROOMS = "inventory_room_snapshot"
TABLE_DWELLING_SIZE = "inventory_dwelling_size_snapshot"

# Expected input parameter rows.
# These are the canonical fire_parameter values in the inputs sheet.
KNOWN_FIRE_PARAMETERS = {
    "fire_spread_category",
    "room_of_origin",
    "fire_area_m2",
    "smoke_heat_damage_area_m2",
    "room_of_origin_size_m2",
    "dwelling_size_m2",
    "dwelling_type",
    "ignition_source_category",
    "ignition_source",
    "input_notes",
}

# Input parameters that should contain text values.
TEXT_FIRE_PARAMETERS = {
    "fire_spread_category",
    "room_of_origin",
    "dwelling_type",
    "ignition_source_category",
    "ignition_source",
    "input_notes",
}

# Input parameters that should contain numeric values.
NUMERIC_FIRE_PARAMETERS = {
    "fire_area_m2",
    "smoke_heat_damage_area_m2",
    "room_of_origin_size_m2",
    "dwelling_size_m2",
}

# Numeric units currently accepted by the fire input file.
# We can expand this later if other unit domains are added.
VALID_NUMERIC_UNITS = {
    "m2",
}

VALID_SINGLE_ITEM_STATUSES = {
    "direct_inventory_item",
    "proxy_inventory_item",
    "invalid_single_item",
    "unmapped",
}

MODEL_READY_SINGLE_ITEM_STATUSES = {
    "direct_inventory_item",
    "proxy_inventory_item",
}

# Canonical fire-spread names currently accepted after input_mapping resolution.
# This includes the current workbook's terms and a couple of future aliases.
KNOWN_CANONICAL_FIRE_SPREAD = {
    "heat_smoke",
    "single_item",
    "single_room",
    "within_room",
    "multiple_rooms",
    "whole_dwelling",
    "entire_dwelling",
}


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------

@dataclass
class FireParameterRow:
    """One row from the inputs sheet."""

    input_row: int
    fire_parameter: str
    value_text: str | None = None
    value_numeric: float | None = None
    unit: str | None = None


@dataclass
class InputValueMappingRow:
    """One row from the input_mapping sheet."""

    mapping_row: int
    input_value: str
    canonical_value: str
    name_category: str


@dataclass
class IgnitionItemMappingRow:
    """One row from the item_mapping sheet."""

    mapping_row: int
    ignition_source: str
    ignition_source_category: str | None
    single_item_status: str
    item_combusted: str | None
    mapping_notes: str | None


@dataclass
class FireEventFilePlan:
    """Dry-run result for one fire input workbook."""

    file_path: str
    file_name: str

    input_rows: list[FireParameterRow] = field(default_factory=list)
    input_mapping_rows: list[InputValueMappingRow] = field(default_factory=list)
    item_mapping_rows: list[IgnitionItemMappingRow] = field(default_factory=list)

    errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[dict[str, Any]] = field(default_factory=list)

    @property
    def has_blocking_errors(self) -> bool:
        """Return True if any collected error should block --apply."""
        return has_blocking_errors(self.errors)


# -----------------------------------------------------------------------------
# Public ingest interface
# -----------------------------------------------------------------------------

def scan_inputs(raw_dir: str | Path) -> list[Path]:
    """
    Strict scanner for the fire input parameter workbook.

    The fire event input ingester expects exactly one controlled workbook:

        fire_input_param.xlsm
    """
    raw_dir = Path(raw_dir)
    target = raw_dir / EXPECTED_FILENAME

    if not target.exists():
        raise FileNotFoundError(
            f"Fire input parameter workbook not found: {target}\n"
            f"Expected exactly one file named '{EXPECTED_FILENAME}' in the fire_event raw directory."
        )

    return [target]


def plan(
    db_path: str | Path,
    raw_dir: str | Path,
    input_files: list[str | Path],
) -> dict[str, Any]:
    """
    Plan fire event parameter ingestion.

    This performs a dry-run parse and validation of the controlled workbook.
    No database writes are made here.
    """
    _ = Path(raw_dir)  # Kept for the shared ingester interface.

    input_file = _validate_single_fire_input_file(input_files)

    conn = db_connect(Path(db_path))
    conn.row_factory = sqlite3.Row

    try:
        file_plan = plan_one_file(conn, input_file)
        counts = _count_existing_rows(conn)

        return {
            "new": [input_file],
            "already_ingested": counts["rows_total"],
            "details": [summarise_plan_result(file_plan)],
        }

    finally:
        conn.close()


def prune_preview(db_path: str | Path, raw_dir: str | Path) -> list[Any]:
    """
    No prune concept for fire_event input.

    The current workbook is treated as the current parameter-input snapshot.
    Re-ingesting replaces previous fire_event staging rows.
    """
    _ = db_path, raw_dir
    return []


def prune_apply(db_path: str | Path, raw_dir: str | Path) -> dict[str, Any]:
    """
    Separate prune apply is not implemented for fire_event input.
    """
    _ = db_path, raw_dir
    return {"rows_deleted": 0, "note": "not applicable"}


def ingest_apply(
    db_path: str | Path,
    raw_dir: str | Path,
    new_files: list[str | Path],
) -> dict[str, Any]:
    """
    Apply fire event parameter ingestion.

    This is a replace-all ingest for the staged fire input workbook:
        - previous fire_event staged input rows are deleted
        - previous fire_event mapping rows are deleted
        - the current workbook contents are inserted

    This makes repeated imports idempotent during development.
    """
    _ = Path(raw_dir)  # Kept for shared interface consistency.
    input_file = _validate_single_fire_input_file(new_files)

    conn = db_connect(Path(db_path))
    conn.row_factory = sqlite3.Row

    started = utc_now_iso()

    try:
        file_plan = plan_one_file(conn, input_file)

        if file_plan.has_blocking_errors:
            try:
                record_ingest_run(
                    conn,
                    IngestLogEntry(
                        data_source_type=SOURCE_TYPE,
                        action="ingest",
                        status="failed",
                        message="blocking validation errors",
                        file_path=file_plan.file_path,
                        file_name=file_plan.file_name,
                        started_utc=started,
                        finished_utc=utc_now_iso(),
                    ),
                )
                conn.commit()
            except Exception:
                pass

            return {
                "file": file_plan.file_name,
                "applied": False,
                "reason": "blocking validation errors",
                "summary": summarise_plan_result(file_plan),
            }

        conn.execute("BEGIN")

        # Replace the previous fire_event staging import.
        delete_existing_fire_event_ingest(conn)

        source_id = insert_source_row(conn, input_file)

        insert_input_value_mapping_rows(
            conn=conn,
            rows=file_plan.input_mapping_rows,
        )

        insert_ignition_item_mapping_rows(
            conn=conn,
            rows=file_plan.item_mapping_rows,
        )

        insert_fire_parameter_rows(
            conn=conn,
            source_id=source_id,
            rows=file_plan.input_rows,
        )

        conn.commit()

        rows_inserted = (
            len(file_plan.input_rows)
            + len(file_plan.input_mapping_rows)
            + len(file_plan.item_mapping_rows)
        )

        # Ingest logging
        try:
            record_ingest_run(
                conn,
                IngestLogEntry(
                    source_id=source_id,
                    data_source_type=SOURCE_TYPE,
                    action="ingest",
                    status="success",
                    message=(
                        f"Imported fire input parameter workbook with "
                        f"{len(file_plan.input_rows)} input rows, "
                        f"{len(file_plan.input_mapping_rows)} input mapping rows, and "
                        f"{len(file_plan.item_mapping_rows)} item mapping rows."
                    ),
                    file_path=file_plan.file_path,
                    file_name=file_plan.file_name,
                    started_utc=started,
                    finished_utc=utc_now_iso(),
                    rows_inserted=rows_inserted,
                ),
            )
            conn.commit()
        except Exception:
            pass

        return {
            "file": file_plan.file_name,
            "applied": True,
            "source_id": source_id,
            "input_rows": len(file_plan.input_rows),
            "input_mapping_rows": len(file_plan.input_mapping_rows),
            "item_mapping_rows": len(file_plan.item_mapping_rows),
            "warnings": file_plan.warnings,
        }

    except Exception as exc:
        conn.rollback()

        try:
            record_ingest_run(
                conn,
                IngestLogEntry(
                    data_source_type=SOURCE_TYPE,
                    action="ingest",
                    status="failed",
                    message=str(exc),
                    file_path=str(input_file),
                    file_name=Path(input_file).name,
                    started_utc=started,
                    finished_utc=utc_now_iso(),
                ),
            )
            conn.commit()
        except Exception:
            pass

        raise

    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Core planning logic
# -----------------------------------------------------------------------------

def plan_one_file(
    conn: sqlite3.Connection,
    input_file: str | Path,
) -> FireEventFilePlan:
    """
    Read, validate, and stage candidate rows from one fire input workbook.
    """
    path = Path(input_file)

    out = FireEventFilePlan(
        file_path=str(path),
        file_name=path.name,
    )

    if not path.exists():
        out.errors.append(error_record("missing_file", file=str(path)))
        return out

    # Validate required destination tables before reading too far.
    validate_destination_schema(conn, out.errors)

    # Inventory snapshots are required for item/room/dwelling validation.
    validate_inventory_snapshot_exists(conn, out.errors)

    # If the schema is missing, stop early to avoid confusing errors.
    if out.has_blocking_errors:
        return out

    try:
        out.input_mapping_rows = read_input_mapping_sheet(path, out.errors)
        out.item_mapping_rows = read_item_mapping_sheet(path, out.errors)
        out.input_rows = read_inputs_sheet(path, out.errors, out.warnings)
    except Exception as exc:
        out.errors.append(error_record("read_excel_failed", file=str(path), detail=str(exc)))
        return out

    # Stop here if the workbook itself could not be read/parsed sensibly.
    if out.has_blocking_errors:
        return out

    # Load DB lookups used for validation.
    current_inventory_snapshot_id = get_current_inventory_snapshot_id(conn)
    item_names = load_current_item_names(conn, current_inventory_snapshot_id)
    room_descriptions = load_current_room_descriptions(conn, current_inventory_snapshot_id)
    dwelling_types = load_current_dwelling_types(conn, current_inventory_snapshot_id)

    # Validate mappings against the inventory snapshot.
    validate_input_mapping_rows(out.input_mapping_rows, out.errors)
    validate_item_mapping_rows(out.item_mapping_rows, item_names, out.errors)

    # Validate input values against mappings and snapshot vocab.
    validate_fire_parameter_rows(
        input_rows=out.input_rows,
        input_mapping_rows=out.input_mapping_rows,
        item_mapping_rows=out.item_mapping_rows,
        room_descriptions=room_descriptions,
        dwelling_types=dwelling_types,
        errors=out.errors,
        warnings=out.warnings,
    )

    return out


# -----------------------------------------------------------------------------
# Workbook readers
# -----------------------------------------------------------------------------

def read_inputs_sheet(
    xlsx_path: str | Path,
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> list[FireParameterRow]:
    """
    Read the `inputs` sheet.

    Expected columns:
        A: fire_parameter
        B: value_text
        C: value_numeric
        D: unit
    """
    df = pd.read_excel(
        xlsx_path,
        sheet_name=SHEET_INPUTS,
        engine="openpyxl",
        dtype=object,
    )

    df.columns = [str(c).strip() for c in df.columns]

    required = ["fire_parameter", "value_text", "value_numeric", "unit"]
    require_columns(df, required, SHEET_INPUTS)

    rows: list[FireParameterRow] = []

    # Drop rows where the parameter itself is blank.
    df = df.loc[~df["fire_parameter"].map(is_blank)].copy()

    seen_params: set[str] = set()

    for idx, r in df.iterrows():
        # Excel row number = DataFrame index + header row + 1
        input_row = int(idx) + 2

        fire_parameter = clean_key(r["fire_parameter"])

        if fire_parameter in seen_params:
            errors.append(error_record(
                "duplicate_fire_parameter",
                sheet=SHEET_INPUTS,
                input_row=input_row,
                fire_parameter=fire_parameter,
            ))
            continue

        seen_params.add(fire_parameter)

        if fire_parameter not in KNOWN_FIRE_PARAMETERS:
            errors.append(error_record(
                "unknown_fire_parameter",
                sheet=SHEET_INPUTS,
                input_row=input_row,
                fire_parameter=fire_parameter,
            ))

        raw_text = r["value_text"]
        raw_numeric = r["value_numeric"]
        raw_unit = r["unit"]

        has_text = not is_blank(raw_text)
        has_numeric = not is_blank(raw_numeric)

        if has_text and has_numeric:
            errors.append(error_record(
                "multiple_value_columns_filled",
                sheet=SHEET_INPUTS,
                input_row=input_row,
                fire_parameter=fire_parameter,
                detail="Only one of value_text or value_numeric should be filled.",
            ))
            continue

        value_text = clean_text(raw_text) if has_text else None
        value_numeric = None
        unit = clean_text(raw_unit) if not is_blank(raw_unit) else None

        if has_numeric:
            value_numeric = coerce_numeric(
                raw_numeric,
                sheet=SHEET_INPUTS,
                input_row=input_row,
                column="value_numeric",
                errors=errors,
            )

            if unit is None:
                errors.append(error_record(
                    "missing_numeric_unit",
                    sheet=SHEET_INPUTS,
                    input_row=input_row,
                    fire_parameter=fire_parameter,
                    detail="Numeric inputs must have a unit in the unit column.",
                ))

            elif unit not in VALID_NUMERIC_UNITS:
                errors.append(error_record(
                    "invalid_numeric_unit",
                    sheet=SHEET_INPUTS,
                    input_row=input_row,
                    fire_parameter=fire_parameter,
                    unit=unit,
                    allowed=sorted(VALID_NUMERIC_UNITS),
                ))

        if fire_parameter in TEXT_FIRE_PARAMETERS and value_numeric is not None:
            errors.append(error_record(
                "unexpected_numeric_value",
                sheet=SHEET_INPUTS,
                input_row=input_row,
                fire_parameter=fire_parameter,
            ))

        if fire_parameter in NUMERIC_FIRE_PARAMETERS and value_text is not None:
            errors.append(error_record(
                "unexpected_text_value",
                sheet=SHEET_INPUTS,
                input_row=input_row,
                fire_parameter=fire_parameter,
            ))

        if value_numeric is not None and value_numeric < 0:
            errors.append(error_record(
                "negative_numeric_value",
                sheet=SHEET_INPUTS,
                input_row=input_row,
                fire_parameter=fire_parameter,
                value=value_numeric,
            ))

        if value_text is not None and unit is not None:
            warnings.append({
                "type": "unit_present_for_text_value",
                "sheet": SHEET_INPUTS,
                "input_row": input_row,
                "fire_parameter": fire_parameter,
                "unit": unit,
                "detail": "Unit value is ignored for text inputs.",
            })

        rows.append(FireParameterRow(
            input_row=input_row,
            fire_parameter=fire_parameter,
            value_text=value_text,
            value_numeric=value_numeric,
            unit=unit,
        ))

    return rows


def read_input_mapping_sheet(
    xlsx_path: str | Path,
    errors: list[dict[str, Any]],
) -> list[InputValueMappingRow]:
    """
    Read the `input_mapping` sheet.

    Expected columns:
        Input
        Canonical naming
        name_category
    """
    df = pd.read_excel(
        xlsx_path,
        sheet_name=SHEET_INPUT_MAPPING,
        engine="openpyxl",
        dtype=object,
    )

    df.columns = [str(c).strip() for c in df.columns]

    required = ["Input", "Canonical naming", "name_category"]
    require_columns(df, required, SHEET_INPUT_MAPPING)

    df = df.loc[~df["Input"].map(is_blank)].copy()

    rows: list[InputValueMappingRow] = []
    seen: set[tuple[str, str]] = set()

    for idx, r in df.iterrows():
        mapping_row = int(idx) + 2

        input_value = clean_text(r["Input"])
        canonical_value = clean_key(r["Canonical naming"])
        name_category = clean_key(r["name_category"])

        if is_blank(input_value) or is_blank(canonical_value) or is_blank(name_category):
            errors.append(error_record(
                "blank_input_mapping_value",
                sheet=SHEET_INPUT_MAPPING,
                mapping_row=mapping_row,
            ))
            continue

        key = (name_category, input_value)
        if key in seen:
            errors.append(error_record(
                "duplicate_input_mapping",
                sheet=SHEET_INPUT_MAPPING,
                mapping_row=mapping_row,
                name_category=name_category,
                input_value=input_value,
            ))
            continue

        seen.add(key)

        rows.append(InputValueMappingRow(
            mapping_row=mapping_row,
            input_value=input_value,
            canonical_value=canonical_value,
            name_category=name_category,
        ))

    return rows


def read_item_mapping_sheet(
    xlsx_path: str | Path,
    errors: list[dict[str, Any]],
) -> list[IgnitionItemMappingRow]:
    """
    Read the `item_mapping` sheet.

    Expected columns:
        FRIS_ignition_source_naming
        FRIS_ignition_category
        single_item_status
        item_combusted
        notes
    """
    df = pd.read_excel(
        xlsx_path,
        sheet_name=SHEET_ITEM_MAPPING,
        engine="openpyxl",
        dtype=object,
    )

    df.columns = [str(c).strip() for c in df.columns]

    required = [
        "FRIS_ignition_source_naming",
        "FRIS_ignition_category",
        "single_item_status",
        "item_combusted",
    ]
    require_columns(df, required, SHEET_ITEM_MAPPING)

    if "notes" not in df.columns:
        df["notes"] = None

    df = df.loc[~df["FRIS_ignition_source_naming"].map(is_blank)].copy()

    rows: list[IgnitionItemMappingRow] = []
    seen: set[tuple[str, str]] = set()

    for idx, r in df.iterrows():
        mapping_row = int(idx) + 2

        ignition_source = clean_text(r["FRIS_ignition_source_naming"])
        ignition_source_category = (
            None
            if is_blank(r["FRIS_ignition_category"])
            else clean_text(r["FRIS_ignition_category"])
        )
        single_item_status = clean_key(r["single_item_status"])

        item_combusted = (
            None
            if is_blank(r["item_combusted"])
            else clean_key(r["item_combusted"])
        )

        mapping_notes = (
            None
            if is_blank(r["notes"])
            else clean_text(r["notes"])
        )

        key = (ignition_source_category or "", ignition_source)
        if key in seen:
            errors.append(error_record(
                "duplicate_ignition_mapping",
                sheet=SHEET_ITEM_MAPPING,
                mapping_row=mapping_row,
                ignition_source_category=ignition_source_category,
                ignition_source=ignition_source,
            ))
            continue

        seen.add(key)

        rows.append(IgnitionItemMappingRow(
            mapping_row=mapping_row,
            ignition_source=ignition_source,
            ignition_source_category=ignition_source_category,
            single_item_status=single_item_status,
            item_combusted=item_combusted,
            mapping_notes=mapping_notes,
        ))

    return rows


# -----------------------------------------------------------------------------
# Validation helpers
# -----------------------------------------------------------------------------

def validate_destination_schema(
    conn: sqlite3.Connection,
    errors: list[dict[str, Any]],
) -> None:
    """
    Check that the fire database has the tables required by this ingester.
    """
    existing = list_tables(conn)

    required = {
        TABLE_SOURCES,
        "ingest_log",
        TABLE_EVENT_PARAMS,
        TABLE_INPUT_MAPPING,
        TABLE_ITEM_MAPPING,
        TABLE_INVENTORY_SNAPSHOT,
        TABLE_INVENTORY_ITEMS,
        TABLE_INVENTORY_ROOMS,
        TABLE_DWELLING_SIZE,
    }

    missing = sorted(required - existing)

    for table in missing:
        errors.append(error_record(
            "missing_destination_table",
            table=table,
            detail="Run or update scripts.fire.init_fire_db first.",
        ))


def validate_inventory_snapshot_exists(
    conn: sqlite3.Connection,
    errors: list[dict[str, Any]],
) -> None:
    """
    Check that inventory snapshot lookup tables have been populated.
    """
    if not table_exists(conn, TABLE_INVENTORY_SNAPSHOT):
        return

    n_snapshots = count_rows(conn, TABLE_INVENTORY_SNAPSHOT)
    n_items = count_rows(conn, TABLE_INVENTORY_ITEMS) if table_exists(conn, TABLE_INVENTORY_ITEMS) else 0
    n_rooms = count_rows(conn, TABLE_INVENTORY_ROOMS) if table_exists(conn, TABLE_INVENTORY_ROOMS) else 0
    n_dwelling = count_rows(conn, TABLE_DWELLING_SIZE) if table_exists(conn, TABLE_DWELLING_SIZE) else 0

    if n_snapshots == 0:
        errors.append(error_record(
            "missing_inventory_snapshot",
            detail="Run scripts.fire.inventory_snapshot before ingesting fire event inputs.",
        ))

    if n_items == 0:
        errors.append(error_record(
            "empty_inventory_item_snapshot",
            detail="No inventory item snapshot rows are available for item_mapping validation.",
        ))

    if n_rooms == 0:
        errors.append(error_record(
            "empty_inventory_room_snapshot",
            detail="No inventory room snapshot rows are available for room_of_origin validation.",
        ))

    if n_dwelling == 0:
        errors.append(error_record(
            "empty_inventory_dwelling_size_snapshot",
            detail="No dwelling size snapshot rows are available for dwelling_type validation.",
        ))


def validate_input_mapping_rows(
    rows: list[InputValueMappingRow],
    errors: list[dict[str, Any]],
) -> None:
    """
    Validate the input value mappings.

    These are mappings from user-facing values to canonical model values.
    """
    for row in rows:
        if row.name_category not in KNOWN_FIRE_PARAMETERS:
            errors.append(error_record(
                "input_mapping_unknown_name_category",
                sheet=SHEET_INPUT_MAPPING,
                mapping_row=row.mapping_row,
                name_category=row.name_category,
            ))

        if row.name_category == "fire_spread_category":
            if row.canonical_value not in KNOWN_CANONICAL_FIRE_SPREAD:
                errors.append(error_record(
                    "unknown_canonical_fire_spread_category",
                    sheet=SHEET_INPUT_MAPPING,
                    mapping_row=row.mapping_row,
                    canonical_value=row.canonical_value,
                    allowed=sorted(KNOWN_CANONICAL_FIRE_SPREAD),
                ))


def validate_item_mapping_rows(
    rows: list[IgnitionItemMappingRow],
    item_names: set[str],
    errors: list[dict[str, Any]],
) -> None:
    """
    Validate ignition-source mappings against the current inventory item snapshot.
    """
    for row in rows:
        if row.single_item_status not in VALID_SINGLE_ITEM_STATUSES:
            errors.append(error_record(
                "invalid_single_item_status",
                sheet=SHEET_ITEM_MAPPING,
                mapping_row=row.mapping_row,
                ignition_source=row.ignition_source,
                single_item_status=row.single_item_status,
                allowed=sorted(VALID_SINGLE_ITEM_STATUSES),
            ))
            continue

        if row.single_item_status in MODEL_READY_SINGLE_ITEM_STATUSES:
            if row.item_combusted is None:
                errors.append(error_record(
                    "missing_item_combusted",
                    sheet=SHEET_ITEM_MAPPING,
                    mapping_row=row.mapping_row,
                    ignition_source=row.ignition_source,
                    single_item_status=row.single_item_status,
                ))

            elif row.item_combusted not in item_names:
                errors.append(error_record(
                    "item_combusted_not_in_inventory_snapshot",
                    sheet=SHEET_ITEM_MAPPING,
                    mapping_row=row.mapping_row,
                    ignition_source=row.ignition_source,
                    item_combusted=row.item_combusted,
                ))

        if row.single_item_status in {"invalid_single_item", "unmapped"}:
            if row.item_combusted is not None:
                errors.append(error_record(
                    "item_combusted_should_be_blank",
                    sheet=SHEET_ITEM_MAPPING,
                    mapping_row=row.mapping_row,
                    ignition_source=row.ignition_source,
                    single_item_status=row.single_item_status,
                    item_combusted=row.item_combusted,
                ))


def validate_fire_parameter_rows(
    input_rows: list[FireParameterRow],
    input_mapping_rows: list[InputValueMappingRow],
    item_mapping_rows: list[IgnitionItemMappingRow],
    room_descriptions: set[str],
    dwelling_types: set[str],
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> None:
    """
    Validate staged input rows against mapping tables and inventory snapshots.
    """
    _ = warnings

    values = {r.fire_parameter: r for r in input_rows}

    # Build easy lookup from input_mapping:
    #   (name_category, input_value) -> canonical_value
    input_value_map = {
        (r.name_category, r.input_value): r.canonical_value
        for r in input_mapping_rows
    }

    # Build ignition mapping lookup.
    # Use category + source because the FRIS list includes duplicate source names.
    ignition_map = {
        (r.ignition_source_category or "", r.ignition_source): r
        for r in item_mapping_rows
    }

    # Always require fire_spread_category.
    fire_spread_raw = get_text_value(values, "fire_spread_category")
    if fire_spread_raw is None:
        errors.append(error_record(
            "missing_required_input",
            fire_parameter="fire_spread_category",
        ))
        return

    fire_spread_canonical = input_value_map.get(
        ("fire_spread_category", fire_spread_raw)
    )

    if fire_spread_canonical is None:
        errors.append(error_record(
            "unmapped_input_value",
            fire_parameter="fire_spread_category",
            value_text=fire_spread_raw,
            detail="fire_spread_category must appear in input_mapping.",
        ))
        return
        

    # Room of origin: mapped later via inventory_room_snapshot.room_description.
    room_of_origin = get_text_value(values, "room_of_origin")
    if room_of_origin is not None and room_of_origin not in room_descriptions:
        errors.append(error_record(
            "room_of_origin_not_in_inventory_snapshot",
            fire_parameter="room_of_origin",
            value_text=room_of_origin,
        ))

    # Dwelling type: uses input_mapping first, then validates canonical value.
    dwelling_type_raw = get_text_value(values, "dwelling_type")
    if dwelling_type_raw is not None:
        dwelling_type_canonical = input_value_map.get(("dwelling_type", dwelling_type_raw))

        if dwelling_type_canonical is None:
            errors.append(error_record(
                "unmapped_input_value",
                fire_parameter="dwelling_type",
                value_text=dwelling_type_raw,
                detail="dwelling_type must appear in input_mapping.",
            ))

        elif dwelling_type_canonical not in dwelling_types:
            errors.append(error_record(
                "dwelling_type_not_in_inventory_snapshot",
                fire_parameter="dwelling_type",
                value_text=dwelling_type_raw,
                canonical_value=dwelling_type_canonical,
            ))

    # Ignition source/category validation.
    ignition_category = get_text_value(values, "ignition_source_category")
    ignition_source = get_text_value(values, "ignition_source")

    if ignition_source is not None:
        if ignition_category is None:
            errors.append(error_record(
                "missing_ignition_source_category",
                fire_parameter="ignition_source",
                value_text=ignition_source,
            ))
        else:
            item_mapping = ignition_map.get((ignition_category, ignition_source))
            if item_mapping is None:
                errors.append(error_record(
                    "ignition_source_not_in_item_mapping",
                    ignition_source_category=ignition_category,
                    ignition_source=ignition_source,
                ))

    # Conditional validation by fire_spread_category.
    validate_fire_spread_specific_inputs(
        values=values,
        fire_spread_canonical=fire_spread_canonical,
        ignition_category=ignition_category,
        ignition_source=ignition_source,
        ignition_map=ignition_map,
        errors=errors,
        warnings=warnings,
    )


def validate_fire_spread_specific_inputs(
    values: dict[str, FireParameterRow],
    fire_spread_canonical: str,
    ignition_category: str | None,
    ignition_source: str | None,
    ignition_map: dict[tuple[str, str], IgnitionItemMappingRow],
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> None:
    """
    Validate required inputs and simple physical constraints by fire-spread class.

    Notes
    -----
    fire_area_m2 is required for all non-single-item cases.

    room_of_origin_size_m2 and dwelling_size_m2 are useful case-specific
    values, but are allowed to be blank because the later model can fall back
    to mean room/dwelling sizes from the inventory snapshot. This is less
    accurate, so missing values are reported as warnings rather than errors.
    """
    fire_area = get_numeric_value(values, "fire_area_m2")
    smoke_heat_damage_area = get_numeric_value(values, "smoke_heat_damage_area_m2")
    room_size = get_numeric_value(values, "room_of_origin_size_m2")
    dwelling_size = get_numeric_value(values, "dwelling_size_m2")

    # -------------------------------------------------
    # General numeric-input warnings
    # -------------------------------------------------

    if smoke_heat_damage_area is None:
        warnings.append({
            "type": "missing_smoke_heat_damage_area_m2",
            "fire_parameter": "smoke_heat_damage_area_m2",
            "detail": (
                "Smoke/heat damage area is missing. Replacement item calculations "
                "will default to using combusted-item/fire-area data instead. "
                "Include smoke_heat_damage_area_m2 to improve replacement/emobodied "
                "emissions accuracy."
            ),
        })

    if room_size is None:
        warnings.append({
            "type": "missing_room_of_origin_size_m2",
            "fire_parameter": "room_of_origin_size_m2",
            "detail": (
                "Room of origin size is missing. The model will default to using "
                "the mean room size from the inventory snapshot, which may be "
                "significantly different from the actual room size. Include "
                "room_of_origin_size_m2 to improve accuracy."
            ),
        })

    if dwelling_size is None:
        warnings.append({
            "type": "missing_dwelling_size_m2",
            "fire_parameter": "dwelling_size_m2",
            "detail": (
                "Dwelling size is missing. The model will default to using "
                "a mean dwelling size, which may be significantly different from "
                "the actual dwelling size. Include dwelling_size_m2 to improve accuracy."
            ),
        })

    # -------------------------------------------------
    # Fire area requirement
    # -------------------------------------------------

    if fire_spread_canonical != "single_item":
        if fire_area is None or fire_area <= 0:
            errors.append(error_record(
                "missing_or_zero_fire_area_for_non_single_item",
                fire_parameter="fire_area_m2",
                fire_spread_category=fire_spread_canonical,
                detail=(
                    "fire_area_m2 must contain a non-zero value, unless the fire "
                    "is confined to a single item."
                ),
            ))

    # -------------------------------------------------
    # Single-item cases
    # -------------------------------------------------

    if fire_spread_canonical == "single_item":
        if ignition_category is None or ignition_source is None:
            errors.append(error_record(
                "single_item_missing_ignition_source",
                fire_spread_category=fire_spread_canonical,
                detail="single_item cases require ignition_source_category and ignition_source.",
            ))
            return

        item_mapping = ignition_map.get((ignition_category, ignition_source))
        if item_mapping is None:
            return

        if item_mapping.single_item_status == "proxy_inventory_item":
            warnings.append({
                "type": "proxy_inventory_item_used",
                "fire_parameter": "ignition_source",
                "ignition_source_category": ignition_category,
                "ignition_source": ignition_source,
                "single_item_status": item_mapping.single_item_status,
                "detail": (
                    "This exact ignition source does not exist in the inventory "
                    "database, so a similar proxy item will be used to calculate "
                    "the emissions."
                ),
            })

        if item_mapping.single_item_status in {"invalid_single_item", "unmapped"}:
            warnings.append({
                "type": "default_single_item_value_required",
                "fire_parameter": "ignition_source",
                "ignition_source_category": ignition_category,
                "ignition_source": ignition_source,
                "single_item_status": item_mapping.single_item_status,
                "detail": (
                    "This ignition source does not have an assigned carbon stock "
                    "value associated with it. The default single-item emission "
                    "value will be returned instead."
                ),
            })
    # -------------------------------------------------
    # Within-room/simple room-scale checks
    # -------------------------------------------------

    if fire_spread_canonical in {"single_room", "within_room"}:
        # This check can only be performed if both values are supplied.
        # Missing room size is allowed, but warned above.
        if fire_area is not None and room_size is not None:
            if fire_area > room_size:
                errors.append(error_record(
                    "single_room_fire_area_exceeds_room_size",
                    fire_area_m2=fire_area,
                    room_of_origin_size_m2=room_size,
                ))

    # -------------------------------------------------
    # Multiple-room checks
    # -------------------------------------------------

    if fire_spread_canonical == "multiple_rooms":
        # These checks can only be performed where the optional case-specific
        # room/dwelling sizes are provided.
        if room_size is not None and dwelling_size is not None:
            if room_size > dwelling_size:
                errors.append(error_record(
                    "room_size_exceeds_dwelling_size",
                    room_of_origin_size_m2=room_size,
                    dwelling_size_m2=dwelling_size,
                ))

        if fire_area is not None and room_size is not None:
            if fire_area <= room_size:
                errors.append(error_record(
                    "multiple_room_fire_area_not_beyond_origin_room",
                    fire_area_m2=fire_area,
                    room_of_origin_size_m2=room_size,
                ))

        if fire_area is not None and dwelling_size is not None:
            if fire_area > dwelling_size:
                errors.append(error_record(
                    "fire_area_exceeds_dwelling_size",
                    fire_area_m2=fire_area,
                    dwelling_size_m2=dwelling_size,
                ))


# -----------------------------------------------------------------------------
# DB lookup helpers
# -----------------------------------------------------------------------------

def get_current_inventory_snapshot_id(conn: sqlite3.Connection) -> int:
    """
    Return the current inventory_snapshot_id.

    The fire database is currently designed to hold only the current snapshot.
    """
    row = conn.execute(
        """
        SELECT inventory_snapshot_id
        FROM inventory_snapshot
        ORDER BY inventory_snapshot_id DESC
        LIMIT 1
        """
    ).fetchone()

    if row is None:
        raise RuntimeError(
            "No inventory snapshot found. Run scripts.fire.inventory_snapshot first."
        )

    return int(row["inventory_snapshot_id"])


def load_current_item_names(
    conn: sqlite3.Connection,
    inventory_snapshot_id: int,
) -> set[str]:
    """
    Load valid item_name values from the current inventory snapshot.
    """
    rows = conn.execute(
        """
        SELECT item_name
        FROM inventory_item_snapshot
        WHERE inventory_snapshot_id = ?
        """,
        (inventory_snapshot_id,),
    ).fetchall()

    return {str(r["item_name"]) for r in rows}


def load_current_room_descriptions(
    conn: sqlite3.Connection,
    inventory_snapshot_id: int,
) -> set[str]:
    """
    Load valid user-facing room descriptions from the current inventory snapshot.
    """
    rows = conn.execute(
        """
        SELECT room_description
        FROM inventory_room_snapshot
        WHERE inventory_snapshot_id = ?
        """,
        (inventory_snapshot_id,),
    ).fetchall()

    return {str(r["room_description"]) for r in rows}


def load_current_dwelling_types(
    conn: sqlite3.Connection,
    inventory_snapshot_id: int,
) -> set[str]:
    """
    Load valid canonical dwelling_type values from the current inventory snapshot.
    """
    rows = conn.execute(
        """
        SELECT dwelling_type
        FROM inventory_dwelling_size_snapshot
        WHERE inventory_snapshot_id = ?
        """,
        (inventory_snapshot_id,),
    ).fetchall()

    return {str(r["dwelling_type"]) for r in rows}


def _count_existing_rows(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Count existing staged fire_event rows.
    """
    n_params = count_rows(conn, TABLE_EVENT_PARAMS) if table_exists(conn, TABLE_EVENT_PARAMS) else 0
    n_input_map = count_rows(conn, TABLE_INPUT_MAPPING) if table_exists(conn, TABLE_INPUT_MAPPING) else 0
    n_item_map = count_rows(conn, TABLE_ITEM_MAPPING) if table_exists(conn, TABLE_ITEM_MAPPING) else 0

    return {
        "rows_fire_event_parameter_input": n_params,
        "rows_fire_input_value_mapping": n_input_map,
        "rows_fire_ignition_item_mapping": n_item_map,
        "rows_total": n_params + n_input_map + n_item_map,
    }


# -----------------------------------------------------------------------------
# Insert/delete helpers
# -----------------------------------------------------------------------------

def insert_source_row(conn: sqlite3.Connection, input_file: str | Path) -> str:
    """
    Insert one source row representing the current fire input workbook import.
    """
    path = Path(input_file)
    source_id = uuid.uuid4().hex

    conn.execute(
        """
        INSERT INTO sources (
            source_id,
            data_source_type,
            source_description,
            source_org,
            file_name,
            file_path,
            url,
            date_collected,
            date_imported_utc,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_id,
            SOURCE_TYPE,
            SOURCE_DESCRIPTION,
            SOURCE_ORG,
            path.name,
            str(path),
            None,
            None,
            utc_now_iso(),
            "Controlled fire input parameter workbook.",
        ),
    )

    return source_id


def delete_existing_fire_event_ingest(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Delete existing staged fire_event rows.

    This ingester treats fire_input_param.xlsm as the current authoritative
    parameter workbook, so re-ingest replaces previous staged values.
    """
    deleted: dict[str, int] = {}

    for table in (TABLE_EVENT_PARAMS, TABLE_INPUT_MAPPING, TABLE_ITEM_MAPPING):
        cur = conn.execute(f"DELETE FROM {table}")
        deleted[table] = cur.rowcount if cur.rowcount != -1 else 0

    # Do not delete source rows here.
    deleted[TABLE_SOURCES] = 0

    return deleted


def insert_fire_parameter_rows(
    conn: sqlite3.Connection,
    source_id: str,
    rows: list[FireParameterRow],
) -> None:
    """
    Insert staged fire input parameter rows.
    """
    if not rows:
        return

    payload = [
        (
            source_id,
            r.input_row,
            r.fire_parameter,
            r.value_text,
            r.value_numeric,
            None,  # value_bool retained for schema compatibility/future use
            r.unit,
            None,  # input_notes retained for future use
        )
        for r in rows
    ]

    conn.executemany(
        """
        INSERT INTO fire_event_parameter_input (
            source_id,
            input_row,
            fire_parameter,
            value_text,
            value_numeric,
            value_bool,
            unit,
            input_notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )


def insert_input_value_mapping_rows(
    conn: sqlite3.Connection,
    rows: list[InputValueMappingRow],
) -> None:
    """
    Insert input value mapping rows.

    These mappings are treated as current fire-model configuration values,
    not as source-specific observation rows, so they do not carry source_id.
    """
    if not rows:
        return

    payload = [
        (
            r.mapping_row,
            r.input_value,
            r.canonical_value,
            r.name_category,
        )
        for r in rows
    ]

    conn.executemany(
        """
        INSERT INTO fire_input_value_mapping (
            mapping_row,
            input_value,
            canonical_value,
            name_category
        ) VALUES (?, ?, ?, ?)
        """,
        payload,
    )


def insert_ignition_item_mapping_rows(
    conn: sqlite3.Connection,
    rows: list[IgnitionItemMappingRow],
) -> None:
    """
    Insert ignition-source-to-item mapping rows.

    These mappings are treated as current fire-model configuration values,
    not as source-specific observation rows, so they do not carry source_id.
    """
    if not rows:
        return

    payload = [
        (
            r.mapping_row,
            r.ignition_source,
            r.ignition_source_category,
            r.single_item_status,
            r.item_combusted,
            r.mapping_notes,
        )
        for r in rows
    ]

    conn.executemany(
        """
        INSERT INTO fire_ignition_item_mapping (
            mapping_row,
            ignition_source,
            ignition_source_category,
            single_item_status,
            item_combusted,
            mapping_notes
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        payload,
    )


# -----------------------------------------------------------------------------
# Small validation/type helpers
# -----------------------------------------------------------------------------

def _validate_single_fire_input_file(input_files: list[str | Path]) -> Path:
    """
    Enforce that fire_event ingestion accepts one controlled workbook.
    """
    if len(input_files) != 1:
        raise ValueError(
            f"Fire event ingester expects exactly one file: {EXPECTED_FILENAME}. "
            f"Got {len(input_files)} file(s): {[str(p) for p in input_files]}"
        )

    p = Path(input_files[0])

    if p.name.lower() != EXPECTED_FILENAME.lower():
        raise ValueError(
            f"Fire event ingester only accepts '{EXPECTED_FILENAME}'. Got: {p.name}"
        )

    if p.suffix.lower() != ".xlsm":
        raise ValueError(f"Fire event ingester expects an .xlsm file. Got: {p}")

    if not p.exists():
        raise FileNotFoundError(f"Input file does not exist: {p}")

    return p


def require_columns(df: pd.DataFrame, required: list[str], sheet: str) -> None:
    """
    Check that an Excel sheet contains required columns.
    """
    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(
            f"Sheet '{sheet}' missing required columns: {missing}. "
            f"Found: {list(df.columns)}"
        )


def require_numeric(
    values: dict[str, FireParameterRow],
    fire_parameter: str,
    fire_spread_category: str,
    errors: list[dict[str, Any]],
) -> None:
    """
    Require a numeric parameter for a given fire spread category.
    """
    if get_numeric_value(values, fire_parameter) is None:
        errors.append(error_record(
            "missing_required_numeric_input",
            fire_parameter=fire_parameter,
            fire_spread_category=fire_spread_category,
        ))


def get_text_value(
    values: dict[str, FireParameterRow],
    fire_parameter: str,
) -> str | None:
    """
    Return a text value from staged rows.
    """
    row = values.get(fire_parameter)
    if row is None:
        return None
    return row.value_text


def get_numeric_value(
    values: dict[str, FireParameterRow],
    fire_parameter: str,
) -> float | None:
    """
    Return a numeric value from staged rows.
    """
    row = values.get(fire_parameter)
    if row is None:
        return None
    return row.value_numeric


def coerce_numeric(
    value: Any,
    sheet: str,
    input_row: int,
    column: str,
    errors: list[dict[str, Any]],
) -> float | None:
    """
    Convert one Excel value to float, reporting structured errors.
    """
    try:
        if isinstance(value, bool):
            raise ValueError("boolean is not numeric")

        numeric = float(value)

        return numeric

    except Exception:
        errors.append(error_record(
            "invalid_numeric_value",
            sheet=sheet,
            input_row=input_row,
            column=column,
            raw_value=repr(value),
        ))
        return None


def is_blank(value: Any) -> bool:
    """
    Treat None, NaN, and empty/whitespace strings as blank.
    """
    if value is None:
        return True

    try:
        if pd.isna(value):
            return True
    except Exception:
        pass

    return str(value).strip() == ""


def clean_text(value: Any) -> str:
    """
    Clean user-facing text while preserving case and punctuation.
    """
    return str(value).strip()


def clean_key(value: Any) -> str:
    """
    Clean canonical key values.

    Canonical values are stored lower-case with trimmed whitespace.
    Spaces are converted to underscores for safety.
    """
    text = str(value).strip().lower()
    text = text.replace(" ", "_")
    return text


def error_record(error_type: str, **kwargs: Any) -> dict[str, Any]:
    """
    Build a structured error dictionary for dry-run reporting.
    """
    out = {"type": error_type}
    out.update(kwargs)
    return out


def has_blocking_errors(errors: list[dict[str, Any]]) -> bool:
    """
    Return True if any collected validation errors should block apply mode.
    """
    blocking = {
        "missing_file",
        "read_excel_failed",
        "missing_destination_table",
        "missing_inventory_snapshot",
        "empty_inventory_item_snapshot",
        "empty_inventory_room_snapshot",
        "empty_inventory_dwelling_size_snapshot",
        "duplicate_fire_parameter",
        "unknown_fire_parameter",
        "multiple_value_columns_filled",
        "missing_numeric_unit",
        "invalid_numeric_unit",
        "unexpected_numeric_value",
        "unexpected_text_value",
        "negative_numeric_value",
        "blank_input_mapping_value",
        "duplicate_input_mapping",
        "input_mapping_unknown_name_category",
        "unknown_canonical_fire_spread_category",
        "duplicate_ignition_mapping",
        "invalid_single_item_status",
        "missing_item_combusted",
        "item_combusted_not_in_inventory_snapshot",
        "item_combusted_should_be_blank",
        "missing_required_input",
        "unmapped_input_value",
        "room_of_origin_not_in_inventory_snapshot",
        "dwelling_type_not_in_inventory_snapshot",
        "missing_ignition_source_category",
        "ignition_source_not_in_item_mapping",
        "single_item_missing_ignition_source",
        "missing_required_numeric_input",
        "missing_or_zero_fire_area_for_non_single_item",
        "single_room_fire_area_exceeds_room_size",
        "room_size_exceeds_dwelling_size",
        "multiple_room_fire_area_not_beyond_origin_room",
        "fire_area_exceeds_dwelling_size",
        "invalid_numeric_value",
    }

    return any(e.get("type") in blocking for e in errors)


# -----------------------------------------------------------------------------
# Generic DB helpers
# -----------------------------------------------------------------------------

def list_tables(conn: sqlite3.Connection) -> set[str]:
    """
    Return table names in the connected database.
    """
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        """
    ).fetchall()

    return {str(r["name"]) for r in rows}


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    """
    Return True if a table exists.
    """
    row = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        """,
        (table_name,),
    ).fetchone()

    return row is not None


def count_rows(conn: sqlite3.Connection, table_name: str) -> int:
    """
    Count rows in a database table.
    """
    row = conn.execute(f"SELECT COUNT(*) AS n FROM {table_name}").fetchone()
    return int(row["n"])


# -----------------------------------------------------------------------------
# Reporting
# -----------------------------------------------------------------------------

def summarise_plan_result(file_plan: FireEventFilePlan) -> dict[str, Any]:
    """
    Convert a FireEventFilePlan into a CLI-friendly summary.
    """
    grouped_errors: dict[str, int] = {}
    for e in file_plan.errors:
        et = str(e.get("type"))
        grouped_errors[et] = grouped_errors.get(et, 0) + 1

    return {
        "file": file_plan.file_name,
        "file_path": file_plan.file_path,
        "input_rows": len(file_plan.input_rows),
        "input_mapping_rows": len(file_plan.input_mapping_rows),
        "item_mapping_rows": len(file_plan.item_mapping_rows),
        "errors": file_plan.errors,
        "error_counts": grouped_errors,
        "warnings": file_plan.warnings,
        "has_blocking_errors": file_plan.has_blocking_errors,
    }