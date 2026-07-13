"""
Ingest the raw FRIS incident-level fire event extract into fire_db/test_db.

This ingester reads the raw FRIS workbook:

    fris_raw.xlsx

and stages the rows directly into:

    input_bulk_fris_events

Notes
-----
This is a raw/staging ingest.

It does NOT calculate fire impacts.
It does NOT map FRIS values to model-facing categories.
It does NOT convert area bands to numeric midpoint values.
It does NOT create final model-facing fire_events rows.

The only transformations performed here are deliberately minimal:

    1. Each workbook row is copied into input_bulk_fris_events.
    2. The FRIS column headings are mapped to the snake_case database fields.
    3. A single source_id is assigned to every row from the imported workbook.

Validation is intentionally light at this stage. The ingester only checks that:

    1. the input file exists
    2. the input file is named fris_raw.xlsx
    3. the workbook has the expected FRIS columns/headings
    4. the workbook contains at least one data row
    5. incident_id values are present and unique, matching the table constraint

Run from the project root through the shared ingest dispatcher, e.g. after adding
this module to scripts/ingest.py:

    # Dry run
    python -m scripts.ingest --profile tom --db test_db --type fris_events --scan

    # Apply
    python -m scripts.ingest --profile tom --db test_db --type fris_events --scan --apply
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

EXPECTED_FILENAME = "fris_raw.xlsx"

# This source type should match the key eventually added to scripts/ingest.py
# and config/local_paths.yaml, e.g. --type fris_events.
SOURCE_TYPE = "fris_events"
SOURCE_DESCRIPTION = "FRIS bulk incident-level fire event extract"
SOURCE_ORG = "FRIS/SFRS"

# Destination tables.
TABLE_SOURCES = "sources"
TABLE_INGEST_LOG = "ingest_log"
TABLE_FRIS_BULK_EVENTS_INPUT = "input_bulk_fris_events"

# Expected raw FRIS workbook columns.
#
# These should match the workbook exactly after trimming leading/trailing
# whitespace from the Excel header cells. The database column names are stored
# separately in FRIS_COLUMN_MAP.
EXPECTED_FRIS_COLUMNS = [
    "Incident_Id",
    "Fiscal_Yr",
    "Property_Type_3",
    "Heat_Smoke_Damage_Only",
    "Ignition_Source_All",
    "Fire_Size_on_Arrival",
    "Fire_Start_Location",
    "Item_First_Ignited",
    "Item_Causing_Spread",
    "Extent_of_Damage",
    "Rapid_Fire_Growth",
    "Building_Room_Origin_Size",
    "Building_Floor_Origin_Size",
    "Building_Fire_Damage_Area",
    "Building_Total_Damage_Area",
    "Distance_to_Adjoining_Property",
]

# Mapping from raw workbook headings to the database field names in
# input_bulk_fris_events.
#
# These are lower-case snake_case names, with symbols/parentheses removed.
FRIS_COLUMN_MAP = {
    "Incident_Id": "incident_id",
    "Fiscal_Yr": "fiscal_yr",
    "Property_Type_3": "property_type_3",
    "Heat_Smoke_Damage_Only": "heat_smoke_damage_only",
    "Ignition_Source_All": "ignition_source_all",
    "Fire_Size_on_Arrival": "fire_size_on_arrival",
    "Fire_Start_Location": "fire_start_location",
    "Item_First_Ignited": "item_first_ignited",
    "Item_Causing_Spread": "item_causing_spread",
    "Extent_of_Damage": "extent_of_damage",
    "Rapid_Fire_Growth": "rapid_fire_growth",
    "Building_Room_Origin_Size": "building_room_origin_size",
    "Building_Floor_Origin_Size": "building_floor_origin_size",
    "Building_Fire_Damage_Area": "building_fire_damage_area",
    "Building_Total_Damage_Area": "building_total_damage_area",
    "Distance_to_Adjoining_Property": "distance_to_adjoining_property",
}


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------

@dataclass
class FrisBulkEventRow:
    """One raw incident row from fris_raw.xlsx, ready for staging."""

    # Excel row number is not inserted into the database because
    # input_bulk_fris_events is intended to be a near-direct copy of the FRIS
    # table plus source_id. It is still kept here for useful error reporting.
    input_row: int

    incident_id: str
    fiscal_yr: str | None = None
    property_type_3: str | None = None
    heat_smoke_damage_only: str | None = None
    ignition_source_all: str | None = None
    fire_size_on_arrival: str | None = None
    fire_start_location: str | None = None
    item_first_ignited: str | None = None
    item_causing_spread: str | None = None
    extent_of_damage: str | None = None
    rapid_fire_growth: str | None = None
    building_room_origin_size: str | None = None
    building_floor_origin_size: str | None = None
    building_fire_damage_area: str | None = None
    building_total_damage_area: str | None = None
    distance_to_adjoining_property: str | None = None


@dataclass
class FrisEventsFilePlan:
    """Dry-run result for one FRIS bulk event workbook."""

    file_path: str
    file_name: str

    rows: list[FrisBulkEventRow] = field(default_factory=list)
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
    Strict scanner for the FRIS raw event workbook.

    The FRIS bulk event ingester expects exactly one controlled workbook:

        fris_raw.xlsx

    The shared ingest dispatcher passes in the raw directory resolved from
    config/local_paths.yaml. For this project, that should resolve to something
    like:

        <sharepoint_root>/fire_db/raw
    """
    raw_dir = Path(raw_dir)
    target = raw_dir / EXPECTED_FILENAME

    if not target.exists():
        raise FileNotFoundError(
            f"FRIS raw event workbook not found: {target}\n"
            f"Expected exactly one file named '{EXPECTED_FILENAME}' in the FRIS raw directory."
        )

    return [target]



def plan(
    db_path: str | Path,
    raw_dir: str | Path,
    input_files: list[str | Path],
) -> dict[str, Any]:
    """
    Plan FRIS bulk event ingestion.

    This performs a dry-run parse and minimal validation of the FRIS workbook.
    No database writes are made here.
    """
    _ = Path(raw_dir)  # Kept for the shared ingester interface.

    input_file = _validate_single_fris_input_file(input_files)

    conn = db_connect(Path(db_path))
    conn.row_factory = sqlite3.Row

    try:
        file_plan = plan_one_file(conn, input_file)
        counts = _count_existing_rows(conn)

        # This ingester treats fris_raw.xlsx as the current raw FRIS snapshot.
        # Therefore, like the single-event input workbook, a scan returns the
        # controlled file as the candidate input. Existing staged rows are
        # replaced only if --apply is used.
        return {
            "new": [input_file],
            "already_ingested": counts["rows_total"],
            "details": [summarise_plan_result(file_plan)],
        }

    finally:
        conn.close()



def prune_preview(db_path: str | Path, raw_dir: str | Path) -> list[Any]:
    """
    No prune concept for FRIS bulk event input.

    The current fris_raw.xlsx workbook is treated as the current FRIS staging
    snapshot. Re-ingesting replaces previous input_bulk_fris_events rows.
    """
    _ = db_path, raw_dir
    return []



def prune_apply(db_path: str | Path, raw_dir: str | Path) -> dict[str, Any]:
    """
    Separate prune apply is not implemented for FRIS bulk event input.
    """
    _ = db_path, raw_dir
    return {"rows_deleted": 0, "note": "not applicable"}



def ingest_apply(
    db_path: str | Path,
    raw_dir: str | Path,
    new_files: list[str | Path],
) -> dict[str, Any]:
    """
    Apply FRIS bulk event ingestion.

    This is a replace-all ingest for the staged FRIS workbook:
        - previous input_bulk_fris_events rows are deleted
        - the current workbook contents are inserted

    This keeps the staging table aligned with the current raw extract while
    preserving source and ingest-log records for auditability.
    """
    _ = Path(raw_dir)  # Kept for shared interface consistency.
    input_file = _validate_single_fris_input_file(new_files)

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

        # Replace the previous FRIS staging import.
        # This deletes only the raw staging table rows, not source records or
        # ingest_log rows.
        delete_existing_fris_events_ingest(conn)

        source_id = insert_source_row(conn, input_file)

        insert_fris_bulk_event_rows(
            conn=conn,
            source_id=source_id,
            rows=file_plan.rows,
        )

        conn.commit()

        rows_inserted = len(file_plan.rows)

        # Ingest logging.
        # The ingest_log helper is deliberately schema-tolerant, so it will
        # insert only the logging fields that exist in the current database.
        try:
            record_ingest_run(
                conn,
                IngestLogEntry(
                    source_id=source_id,
                    data_source_type=SOURCE_TYPE,
                    action="ingest",
                    status="success",
                    message=(
                        f"Imported FRIS bulk event workbook with "
                        f"{rows_inserted} incident row(s)."
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
            "fris_bulk_event_rows": rows_inserted,
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
) -> FrisEventsFilePlan:
    """
    Read, minimally validate, and stage candidate rows from one FRIS workbook.
    """
    path = Path(input_file)

    out = FrisEventsFilePlan(
        file_path=str(path),
        file_name=path.name,
    )

    if not path.exists():
        out.errors.append(error_record("missing_file", file=str(path)))
        return out

    # Validate required destination tables before reading too far.
    validate_destination_schema(conn, out.errors)

    # If the schema is missing, stop early to avoid confusing errors.
    if out.has_blocking_errors:
        return out

    try:
        out.rows = read_fris_events_sheet(path, out.errors, out.warnings)
    except Exception as exc:
        out.errors.append(error_record("read_excel_failed", file=str(path), detail=str(exc)))
        return out

    return out


# -----------------------------------------------------------------------------
# Workbook reader
# -----------------------------------------------------------------------------


def read_fris_events_sheet(
    xlsx_path: str | Path,
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> list[FrisBulkEventRow]:
    """
    Read the first worksheet from fris_raw.xlsx.

    The current FRIS extract is expected to contain one table with the 14 raw
    FRIS columns listed in EXPECTED_FRIS_COLUMNS.

    All cell values are stored as text at this raw staging stage. The resolver
    can later decide how to interpret categories, area bands, and other fields.
    """
    _ = warnings  # Kept for symmetry with other ingesters/future use.

    df = pd.read_excel(
    xlsx_path,
    sheet_name=0,
    engine="openpyxl",
    dtype=object,
    # Disable Pandas' default NA parsing so source values such as "None"
    # remain literal strings. Missing FRIS entries are handled explicitly
    # by clean_cell_value()/is_blank(), where "NULL" and blank cells are
    # converted to SQL NULL but "None" is preserved.
    keep_default_na=False,
    na_values=[],
)

    # Trim header whitespace but otherwise require the expected headings.
    df.columns = [str(c).strip() for c in df.columns]

    validate_fris_columns(df, errors)

    # If the headings are wrong, do not attempt to interpret the rows.
    if has_blocking_errors(errors):
        return []

    if len(df) == 0:
        errors.append(error_record(
            "no_data_rows",
            detail="fris_raw.xlsx must contain a header row and at least one incident row.",
        ))
        return []

    rows: list[FrisBulkEventRow] = []
    seen_incident_ids: dict[str, int] = {}

    for idx, r in df.iterrows():
        # Excel row number = DataFrame index + header row + 1.
        input_row = int(idx) + 2

        staged_values = {
            db_col: clean_cell_value(r[raw_col])
            for raw_col, db_col in FRIS_COLUMN_MAP.items()
        }

        incident_id = staged_values["incident_id"]

        if incident_id is None:
            errors.append(error_record(
                "missing_incident_id",
                input_row=input_row,
                detail="Incident_Id is required because incident_id is the primary key.",
            ))
            continue

        previous_row = seen_incident_ids.get(incident_id)
        if previous_row is not None:
            errors.append(error_record(
                "duplicate_incident_id",
                incident_id=incident_id,
                first_input_row=previous_row,
                duplicate_input_row=input_row,
                detail="Incident_Id values must be unique within fris_raw.xlsx.",
            ))
            continue

        seen_incident_ids[incident_id] = input_row

        rows.append(FrisBulkEventRow(
            input_row=input_row,
            incident_id=incident_id,
            fiscal_yr=staged_values["fiscal_yr"],
            property_type_3=staged_values["property_type_3"],
            heat_smoke_damage_only=staged_values["heat_smoke_damage_only"],
            ignition_source_all=staged_values["ignition_source_all"],
            fire_size_on_arrival=staged_values["fire_size_on_arrival"],
            fire_start_location=staged_values["fire_start_location"],
            item_first_ignited=staged_values["item_first_ignited"],
            item_causing_spread=staged_values["item_causing_spread"],
            extent_of_damage=staged_values["extent_of_damage"],
            rapid_fire_growth=staged_values["rapid_fire_growth"],
            building_room_origin_size=staged_values["building_room_origin_size"],
            building_floor_origin_size=staged_values["building_floor_origin_size"],
            building_fire_damage_area=staged_values["building_fire_damage_area"],
            building_total_damage_area=staged_values["building_total_damage_area"],
            distance_to_adjoining_property=staged_values["distance_to_adjoining_property"],
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
        TABLE_INGEST_LOG,
        TABLE_FRIS_BULK_EVENTS_INPUT,
    }

    missing = sorted(required - existing)

    for table in missing:
        errors.append(error_record(
            "missing_destination_table",
            table=table,
            detail="Run or update scripts.fire.init_fire_db first.",
        ))



def validate_fris_columns(
    df: pd.DataFrame,
    errors: list[dict[str, Any]],
) -> None:
    """
    Check that the FRIS workbook contains exactly the expected headings.

    This intentionally checks both the number of columns and the exact ordered
    heading names. That makes the raw ingest fail fast if the supplied extract
    changes shape.
    """
    found = list(df.columns)

    if found == EXPECTED_FRIS_COLUMNS:
        return

    missing = [c for c in EXPECTED_FRIS_COLUMNS if c not in found]
    extra = [c for c in found if c not in EXPECTED_FRIS_COLUMNS]

    errors.append(error_record(
        "unexpected_columns",
        expected_count=len(EXPECTED_FRIS_COLUMNS),
        found_count=len(found),
        expected_columns=EXPECTED_FRIS_COLUMNS,
        found_columns=found,
        missing_columns=missing,
        extra_columns=extra,
        detail="fris_raw.xlsx must contain the expected FRIS columns in the expected order.",
    ))


# -----------------------------------------------------------------------------
# Insert/delete helpers
# -----------------------------------------------------------------------------


def insert_source_row(conn: sqlite3.Connection, input_file: str | Path) -> str:
    """
    Insert one source row representing the current FRIS workbook import.

    A single generated source_id is then used for every incident row imported
    from this workbook.
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
            "Raw FRIS bulk incident-level fire event workbook.",
        ),
    )

    return source_id



def delete_existing_fris_events_ingest(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Delete existing staged FRIS event rows.

    This ingester treats fris_raw.xlsx as the current authoritative raw FRIS
    extract, so re-ingest replaces previous staged FRIS rows.
    """
    deleted: dict[str, int] = {}

    cur = conn.execute(f"DELETE FROM {TABLE_FRIS_BULK_EVENTS_INPUT}")
    deleted[TABLE_FRIS_BULK_EVENTS_INPUT] = cur.rowcount if cur.rowcount != -1 else 0

    # Do not delete source rows here.
    deleted[TABLE_SOURCES] = 0

    return deleted



def insert_fris_bulk_event_rows(
    conn: sqlite3.Connection,
    source_id: str,
    rows: list[FrisBulkEventRow],
) -> None:
    """
    Insert staged FRIS incident rows.
    """
    if not rows:
        return

    payload = [
        (
            source_id,
            r.incident_id,
            r.fiscal_yr,
            r.property_type_3,
            r.heat_smoke_damage_only,
            r.ignition_source_all,
            r.fire_size_on_arrival,
            r.fire_start_location,
            r.item_first_ignited,
            r.item_causing_spread,
            r.extent_of_damage,
            r.rapid_fire_growth,
            r.building_room_origin_size,
            r.building_floor_origin_size,
            r.building_fire_damage_area,
            r.building_total_damage_area,
            r.distance_to_adjoining_property,
        )
        for r in rows
    ]

    conn.executemany(
        f"""
        INSERT INTO {TABLE_FRIS_BULK_EVENTS_INPUT} (
            source_id,
            incident_id,
            fiscal_yr,
            property_type_3,
            heat_smoke_damage_only,
            ignition_source_all,
            fire_size_on_arrival,
            fire_start_location,
            item_first_ignited,
            item_causing_spread,
            extent_of_damage,
            rapid_fire_growth,
            building_room_origin_size,
            building_floor_origin_size,
            building_fire_damage_area,
            building_total_damage_area,
            distance_to_adjoining_property
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )


# -----------------------------------------------------------------------------
# Small validation/type helpers
# -----------------------------------------------------------------------------


def _validate_single_fris_input_file(input_files: list[str | Path]) -> Path:
    """
    Enforce that FRIS bulk event ingestion accepts one controlled workbook.
    """
    if len(input_files) != 1:
        raise ValueError(
            f"FRIS bulk event ingester expects exactly one file: {EXPECTED_FILENAME}. "
            f"Got {len(input_files)} file(s): {[str(p) for p in input_files]}"
        )

    p = Path(input_files[0])

    if p.name.lower() != EXPECTED_FILENAME.lower():
        raise ValueError(
            f"FRIS bulk event ingester only accepts '{EXPECTED_FILENAME}'. Got: {p.name}"
        )

    if p.suffix.lower() != ".xlsx":
        raise ValueError(f"FRIS bulk event ingester expects an .xlsx file. Got: {p}")

    if not p.exists():
        raise FileNotFoundError(f"Input file does not exist: {p}")

    return p



def clean_cell_value(value: Any) -> str | None:
    """
    Convert one raw Excel cell to a text value suitable for raw staging.

    The FRIS staging table intentionally stores all source columns as TEXT.
    This helper keeps that behaviour explicit while avoiding common Excel/Pandas
    artefacts such as integer-looking IDs becoming '123.0'.
    """
    if is_blank(value):
        return None

    if isinstance(value, bool):
        return str(value).strip()

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return str(value).strip()

    return str(value).strip()



def is_blank(value: Any) -> bool:
    """
    Treat true missing values, blank strings, and explicit FRIS NULL markers
    as blank.

    Important: the literal string "None" is a valid FRIS category/value and
    must be preserved as text, not converted to SQL NULL.
    """
    if value is None:
        return True

    text = str(value).strip()

    if text == "":
        return True

    if text.upper() in {"NULL", "NAN"}:
        return True

    try:
        if pd.isna(value):
            return True
    except Exception:
        pass

    return False



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
        "unexpected_columns",
        "no_data_rows",
        "missing_incident_id",
        "duplicate_incident_id",
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



def _count_existing_rows(conn: sqlite3.Connection) -> dict[str, int]:
    """
    Count existing staged FRIS event rows.
    """
    n_fris_rows = (
        count_rows(conn, TABLE_FRIS_BULK_EVENTS_INPUT)
        if table_exists(conn, TABLE_FRIS_BULK_EVENTS_INPUT)
        else 0
    )

    return {
        "rows_fris_bulk_events_input": n_fris_rows,
        "rows_total": n_fris_rows,
    }


# -----------------------------------------------------------------------------
# Reporting
# -----------------------------------------------------------------------------


def summarise_plan_result(file_plan: FrisEventsFilePlan) -> dict[str, Any]:
    """
    Convert a FrisEventsFilePlan into a CLI-friendly summary.
    """
    grouped_errors: dict[str, int] = {}
    for e in file_plan.errors:
        et = str(e.get("type"))
        grouped_errors[et] = grouped_errors.get(et, 0) + 1

    return {
        "file": file_plan.file_name,
        "file_path": file_plan.file_path,
        "fris_bulk_event_rows": len(file_plan.rows),
        "errors": file_plan.errors,
        "error_counts": grouped_errors,
        "warnings": file_plan.warnings,
        "has_blocking_errors": file_plan.has_blocking_errors,
    }
