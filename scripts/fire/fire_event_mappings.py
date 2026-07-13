from __future__ import annotations

"""
Read, validate, and ingest fire_event_mappings.xlsm into fire_db.

This module contains the main workbook parsing, validation, and database-write
logic for the FRIS/single-event fire-event mapping configuration workbook.

The thin dispatcher-facing wrapper should live in:
    scripts/fire/ingest_fire_event_mappings.py

and delegate to this module, following the same pattern as the inventory vocab
ingester.
"""

import sqlite3
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


# -----------------------------------------------------------------------------
# Workbook / database configuration
# -----------------------------------------------------------------------------

EXPECTED_FILENAME = "fire_event_mappings.xlsm"
SOURCE_TYPE = "fire_mappings"
SOURCE_DESCRIPTION = "Fire event mapping workbook"
SOURCE_ORG = "internal"

SHEET_DWELLINGS = "dwellings"
SHEET_FIRE_CAT = "fire_cat"
SHEET_ITEMS = "items"
SHEET_ITEM_INFERENCE = "item_inference"
SHEET_ROOMS = "rooms"
SHEET_WARNINGS = "warnings"
SHEET_AREA_BANDS = "area_bands"

TABLE_SOURCES = "sources"
TABLE_INGEST_LOG = "ingest_log"
TABLE_WARNINGS = "fire_event_mapping_warnings"
TABLE_DWELLINGS = "fire_event_mapping_dwellings"
TABLE_FIRE_CAT = "fire_event_mapping_fire_cat"
TABLE_ITEMS = "fire_event_mapping_items"
TABLE_ITEM_INFERENCE = "fire_event_mapping_item_inference"
TABLE_ROOMS = "fire_event_mapping_rooms"
TABLE_AREA_BANDS = "fire_event_mapping_area_bands"

TABLE_INVENTORY_SNAPSHOT = "inventory_snapshot"
TABLE_INVENTORY_ITEMS = "inventory_item_snapshot"
TABLE_INVENTORY_ROOMS = "inventory_room_snapshot"
TABLE_INVENTORY_DWELLINGS = "inventory_dwelling_size_snapshot"

MAPPING_TABLES = [
    TABLE_DWELLINGS,
    TABLE_FIRE_CAT,
    TABLE_ITEMS,
    TABLE_ITEM_INFERENCE,
    TABLE_ROOMS,
    TABLE_AREA_BANDS,
    TABLE_WARNINGS,
]

ALLOWED_FIRE_SPREAD_CATEGORIES = {
    "none",
    "heat_smoke_damage_only",
    "single_item",
    "within_room",
    "multiple_rooms",
    "entire_dwelling",
    "roof",
    "unspecified",
}

ALLOWED_SINGLE_ITEM_STATUS = {
    "direct_inventory_item",
    "proxy_inventory_item",
    "conditionally_inferred_item",
    "invalid_single_item",
}

MODEL_READY_SINGLE_ITEM_STATUS = {
    "direct_inventory_item",
    "proxy_inventory_item",
}

ALLOWED_OCCUPANCY = {"single", "multiple", "unknown"}


# -----------------------------------------------------------------------------
# Dataclasses
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class WarningRow:
    mapping_row: int
    warning_category: str | None
    warning_type: str
    warning_text: str
    notes: str | None = None


@dataclass(frozen=True)
class DwellingRow:
    mapping_row: int
    fris_dwelling_naming: str
    dwelling_type: str | None
    dwelling_type_proxy: str | None
    occupancy_override: str | None
    omit_from_model: int
    warning_type: str | None
    notes: str | None


@dataclass(frozen=True)
class FireCategoryRow:
    mapping_row: int
    fris_fire_categories: str
    fire_spread_category: str
    omit_from_model: int
    occupancy_dependent: int
    warning_type: str | None
    conditional_warning: int
    notes: str | None


@dataclass(frozen=True)
class ItemRow:
    mapping_row: int
    ignition_source_all: str
    ignition_source_category_override: str | None
    ignition_source_override: str | None
    single_item_status: str
    item_combusted: str | None
    warning_type: str | None
    notes: str | None


@dataclass(frozen=True)
class ItemInferenceRow:
    mapping_row: int
    ignition_category: str | None
    ignition_source: str
    fire_spread_category: str
    room_type: str | None
    item_first_ignited: str | None
    item_combusted: str
    notes: str | None


@dataclass(frozen=True)
class RoomRow:
    mapping_row: int
    fire_start_location: str
    room_type: str | None
    warning_type: str | None
    notes: str | None


@dataclass(frozen=True)
class AreaBandRow:
    mapping_row: int
    area_band: str
    band_order: int
    is_none_band: int
    is_open_ended: int
    notes: str | None


@dataclass
class FireEventMappingsPlan:
    file_path: str
    file_name: str
    warnings: list[WarningRow] = field(default_factory=list)
    dwellings: list[DwellingRow] = field(default_factory=list)
    fire_cat: list[FireCategoryRow] = field(default_factory=list)
    items: list[ItemRow] = field(default_factory=list)
    item_inference: list[ItemInferenceRow] = field(default_factory=list)
    rooms: list[RoomRow] = field(default_factory=list)
    area_bands: list[AreaBandRow] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    nonblocking_warnings: list[dict[str, Any]] = field(default_factory=list)

    @property
    def has_blocking_errors(self) -> bool:
        return len(self.errors) > 0


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------

def build_plan(db_path: str | Path, xlsx_path: str | Path) -> FireEventMappingsPlan:
    """Read and validate one fire_event_mappings workbook without writing to DB."""
    xlsx_path = Path(xlsx_path)

    out = FireEventMappingsPlan(
        file_path=str(xlsx_path),
        file_name=xlsx_path.name,
    )

    if not xlsx_path.exists():
        out.errors.append(error_record("missing_file", file=str(xlsx_path)))
        return out

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        validate_destination_schema(conn, out.errors)

        if out.has_blocking_errors:
            return out

        current_inventory_snapshot_id = get_current_inventory_snapshot_id(conn, out.errors)
        if current_inventory_snapshot_id is None:
            return out

        item_names = load_current_item_names(conn, current_inventory_snapshot_id)
        room_types = load_current_room_types(conn, current_inventory_snapshot_id)

        try:
            out.warnings = read_warnings_sheet(xlsx_path, out.errors)
            out.dwellings = read_dwellings_sheet(xlsx_path, out.errors)
            out.fire_cat = read_fire_cat_sheet(xlsx_path, out.errors)
            out.items = read_items_sheet(xlsx_path, out.errors)
            out.item_inference = read_item_inference_sheet(xlsx_path, out.errors)
            out.rooms = read_rooms_sheet(xlsx_path, out.errors)
            out.area_bands = read_area_bands_sheet(xlsx_path, out.errors)
        except Exception as exc:
            out.errors.append(error_record("read_excel_failed", file=str(xlsx_path), detail=str(exc)))
            return out

        if out.has_blocking_errors:
            return out

        validate_warning_catalogue(out.warnings, out.errors)
        valid_warning_types = {row.warning_type for row in out.warnings}

        validate_warning_references(out.dwellings, valid_warning_types, SHEET_DWELLINGS, out.errors)
        validate_warning_references(out.fire_cat, valid_warning_types, SHEET_FIRE_CAT, out.errors)
        validate_warning_references(out.items, valid_warning_types, SHEET_ITEMS, out.errors)
        validate_warning_references(out.rooms, valid_warning_types, SHEET_ROOMS, out.errors)

        validate_dwellings(out.dwellings, out.errors)
        validate_fire_cat(out.fire_cat, out.errors)
        validate_items(out.items, item_names, out.errors)
        validate_item_inference(out.item_inference, item_names, room_types, out.errors)
        validate_rooms(out.rooms, room_types, out.errors)
        validate_area_bands(out.area_bands, out.errors)

        return out

    finally:
        conn.close()


def ingest_fire_event_mappings(
    *,
    db_path: str | Path,
    xlsx_path: str | Path,
    mode: str = "replace_all",
) -> dict[str, Any]:
    """Validate and ingest fire_event_mappings.xlsm into fire_event_mapping_* tables."""
    db_path = Path(db_path)
    xlsx_path = Path(xlsx_path)

    if mode != "replace_all":
        raise ValueError("Only mode='replace_all' is currently supported for fire_event_mappings.")

    plan = build_plan(db_path, xlsx_path)
    if plan.has_blocking_errors:
        raise ValueError(format_validation_errors(plan.errors))

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    source_id = uuid.uuid4().hex
    started_utc = utc_now_iso()

    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute("BEGIN;")

        delete_existing_mapping_rows(conn)
        insert_source_row(conn, source_id, xlsx_path)

        insert_warning_rows(conn, plan.warnings)
        insert_dwelling_rows(conn, plan.dwellings)
        insert_fire_cat_rows(conn, plan.fire_cat)
        insert_item_rows(conn, plan.items)
        insert_item_inference_rows(conn, plan.item_inference)
        insert_room_rows(conn, plan.rooms)
        insert_area_band_rows(conn, plan.area_bands)

        rows_inserted = (
            len(plan.warnings)
            + len(plan.dwellings)
            + len(plan.fire_cat)
            + len(plan.items)
            + len(plan.item_inference)
            + len(plan.rooms)
            + len(plan.area_bands)
        )

        insert_ingest_log(
            conn,
            source_id=source_id,
            status="success",
            message=(
                f"Imported fire event mapping workbook with {rows_inserted} mapping rows."
            ),
            started_utc=started_utc,
            finished_utc=utc_now_iso(),
            rows_inserted=rows_inserted,
            rows_deleted=None,
        )

        conn.commit()

        return {
            "file": str(xlsx_path),
            "mode": mode,
            "source_id": source_id,
            "rows_inserted": rows_inserted,
            **count_mapping_rows(conn),
        }

    except Exception:
        conn.rollback()
        try:
            insert_ingest_log(
                conn,
                source_id=None,
                status="failed",
                message="fire_event_mappings ingest failed",
                started_utc=started_utc,
                finished_utc=utc_now_iso(),
                rows_inserted=None,
                rows_deleted=None,
            )
            conn.commit()
        except Exception:
            pass
        raise

    finally:
        conn.close()


def summarise_plan(plan: FireEventMappingsPlan) -> dict[str, Any]:
    """Return a compact CLI-friendly summary for dry-run output."""
    error_counts: dict[str, int] = {}
    for err in plan.errors:
        err_type = str(err.get("type"))
        error_counts[err_type] = error_counts.get(err_type, 0) + 1

    return {
        "file": plan.file_name,
        "file_path": plan.file_path,
        "warnings_rows": len(plan.warnings),
        "dwellings_rows": len(plan.dwellings),
        "fire_cat_rows": len(plan.fire_cat),
        "items_rows": len(plan.items),
        "item_inference_rows": len(plan.item_inference),
        "rooms_rows": len(plan.rooms),
        "area_bands_rows": len(plan.area_bands),
        "errors": plan.errors,
        "error_counts": error_counts,
        "nonblocking_warnings": plan.nonblocking_warnings,
        "has_blocking_errors": plan.has_blocking_errors,
    }


# -----------------------------------------------------------------------------
# Workbook readers
# -----------------------------------------------------------------------------

def read_warnings_sheet(xlsx_path: Path, errors: list[dict[str, Any]]) -> list[WarningRow]:
    df = read_sheet(xlsx_path, SHEET_WARNINGS)
    require_columns(df, ["warning_category", "warning_type", "warning_text"], SHEET_WARNINGS)
    if "notes" not in df.columns:
        df["notes"] = None

    rows: list[WarningRow] = []
    df = df.loc[~df["warning_type"].map(is_blank)].copy()

    for idx, r in df.iterrows():
        mapping_row = excel_row_number(idx)
        warning_type = clean_key(r["warning_type"])
        warning_text = clean_optional_text(r["warning_text"])
        if warning_text is None:
            errors.append(error_record("missing_warning_text", sheet=SHEET_WARNINGS, mapping_row=mapping_row, warning_type=warning_type))
            continue

        rows.append(WarningRow(
            mapping_row=mapping_row,
            warning_category=clean_key_or_none(r["warning_category"]),
            warning_type=warning_type,
            warning_text=warning_text,
            notes=clean_optional_text(r["notes"]),
        ))

    return rows


def read_dwellings_sheet(xlsx_path: Path, errors: list[dict[str, Any]]) -> list[DwellingRow]:
    df = read_sheet(xlsx_path, SHEET_DWELLINGS)
    require_columns(df, [
        "fris_dwelling_naming",
        "dwelling_type",
        "dwelling_type_proxy",
        "occupancy_override",
        "omit_from_model",
        "warning_type",
        "notes",
    ], SHEET_DWELLINGS)

    rows: list[DwellingRow] = []
    df = df.loc[~df["fris_dwelling_naming"].map(is_blank)].copy()

    for idx, r in df.iterrows():
        mapping_row = excel_row_number(idx)
        rows.append(DwellingRow(
            mapping_row=mapping_row,
            fris_dwelling_naming=clean_text(r["fris_dwelling_naming"]),
            dwelling_type=clean_key_or_none(r["dwelling_type"]),
            dwelling_type_proxy=clean_key_or_none(r["dwelling_type_proxy"]),
            occupancy_override=clean_key_or_none(r["occupancy_override"]),
            omit_from_model=coerce_boolish(r["omit_from_model"], default=False, sheet=SHEET_DWELLINGS, mapping_row=mapping_row, column="omit_from_model", errors=errors),
            warning_type=clean_warning_type_field(r["warning_type"]),
            notes=clean_optional_text(r["notes"]),
        ))

    return rows


def read_fire_cat_sheet(xlsx_path: Path, errors: list[dict[str, Any]]) -> list[FireCategoryRow]:
    df = read_sheet(xlsx_path, SHEET_FIRE_CAT)
    require_columns(df, [
        "fris_fire_categories",
        "fire_spread_category",
        "omit_from_model",
        "occupancy_dependent",
        "warning_type",
        "conditional_warning",
        "notes",
    ], SHEET_FIRE_CAT)

    rows: list[FireCategoryRow] = []
    df = df.loc[~df["fris_fire_categories"].map(is_blank)].copy()

    for idx, r in df.iterrows():
        mapping_row = excel_row_number(idx)
        fire_spread_category = clean_key(r["fire_spread_category"])
        rows.append(FireCategoryRow(
            mapping_row=mapping_row,
            fris_fire_categories=clean_text(r["fris_fire_categories"]),
            fire_spread_category=fire_spread_category,
            omit_from_model=coerce_boolish(r["omit_from_model"], default=False, sheet=SHEET_FIRE_CAT, mapping_row=mapping_row, column="omit_from_model", errors=errors),
            occupancy_dependent=coerce_boolish(r["occupancy_dependent"], default=False, sheet=SHEET_FIRE_CAT, mapping_row=mapping_row, column="occupancy_dependent", errors=errors),
            warning_type=clean_warning_type_field(r["warning_type"]),
            conditional_warning=coerce_boolish(r["conditional_warning"], default=False, sheet=SHEET_FIRE_CAT, mapping_row=mapping_row, column="conditional_warning", errors=errors),
            notes=clean_optional_text(r["notes"]),
        ))

    return rows


def read_items_sheet(xlsx_path: Path, errors: list[dict[str, Any]]) -> list[ItemRow]:
    df = read_sheet(xlsx_path, SHEET_ITEMS)
    # Accept the earlier header used in the workbook, but standardise internally.
    rename_aliases(df, {"fris_ignition_source_all": "ignition_source_all"})
    require_columns(df, [
        "ignition_source_all",
        "ignition_source_category_override",
        "ignition_source_override",
        "item_combusted",
        "single_item_status",
        "warning_type",
    ], SHEET_ITEMS)
    if "notes" not in df.columns:
        df["notes"] = None

    rows: list[ItemRow] = []
    df = df.loc[~df["ignition_source_all"].map(is_blank)].copy()

    for idx, r in df.iterrows():
        mapping_row = excel_row_number(idx)
        status = clean_key_or_none(r["single_item_status"]) or "invalid_single_item"
        rows.append(ItemRow(
            mapping_row=mapping_row,
            ignition_source_all=clean_text(r["ignition_source_all"]),
            ignition_source_category_override=clean_optional_text(r["ignition_source_category_override"]),
            ignition_source_override=clean_optional_text(r["ignition_source_override"]),
            single_item_status=status,
            item_combusted=clean_key_or_none(r["item_combusted"]),
            warning_type=clean_warning_type_field(r["warning_type"]),
            notes=clean_optional_text(r["notes"]),
        ))

    return rows


def read_item_inference_sheet(xlsx_path: Path, errors: list[dict[str, Any]]) -> list[ItemInferenceRow]:
    df = read_sheet(xlsx_path, SHEET_ITEM_INFERENCE)
    require_columns(df, [
        "ignition_category",
        "ignition_source",
        "fire_spread_category",
        "room_type",
        "item_first_ignited",
        "item_combusted",
        "notes",
    ], SHEET_ITEM_INFERENCE)

    rows: list[ItemInferenceRow] = []
    df = df.loc[~df["ignition_source"].map(is_blank)].copy()

    for idx, r in df.iterrows():
        mapping_row = excel_row_number(idx)
        item_combusted = clean_key_or_none(r["item_combusted"])
        if item_combusted is None:
            errors.append(error_record("missing_item_inference_item_combusted", sheet=SHEET_ITEM_INFERENCE, mapping_row=mapping_row))
            continue

        rows.append(ItemInferenceRow(
            mapping_row=mapping_row,
            ignition_category=clean_optional_text(r["ignition_category"]),
            ignition_source=clean_text(r["ignition_source"]),
            fire_spread_category=clean_key(r["fire_spread_category"]),
            room_type=clean_key_or_none(r["room_type"]),
            item_first_ignited=clean_optional_text(r["item_first_ignited"]),
            item_combusted=item_combusted,
            notes=clean_optional_text(r["notes"]),
        ))

    return rows


def read_rooms_sheet(xlsx_path: Path, errors: list[dict[str, Any]]) -> list[RoomRow]:
    df = read_sheet(xlsx_path, SHEET_ROOMS)
    require_columns(df, ["fire_start_location", "room_type", "warning_type", "notes"], SHEET_ROOMS)

    rows: list[RoomRow] = []
    df = df.loc[~df["fire_start_location"].map(is_blank)].copy()

    for idx, r in df.iterrows():
        mapping_row = excel_row_number(idx)
        rows.append(RoomRow(
            mapping_row=mapping_row,
            fire_start_location=clean_text(r["fire_start_location"]),
            room_type=clean_key_or_none(r["room_type"]),
            warning_type=clean_warning_type_field(r["warning_type"]),
            notes=clean_optional_text(r["notes"]),
        ))

    return rows


def read_area_bands_sheet(xlsx_path: Path, errors: list[dict[str, Any]]) -> list[AreaBandRow]:
    df = read_sheet(xlsx_path, SHEET_AREA_BANDS)
    rename_aliases(df, {"band_position": "band_order"})
    require_columns(df, ["area_band", "band_order"], SHEET_AREA_BANDS)

    if "is_none_band" not in df.columns:
        df["is_none_band"] = None
    if "is_open_ended" not in df.columns:
        df["is_open_ended"] = None
    if "notes" not in df.columns:
        df["notes"] = None
    if "low_value" not in df.columns:
        df["low_value"] = None
    if "high_value" not in df.columns:
        df["high_value"] = None

    rows: list[AreaBandRow] = []

    for idx, r in df.iterrows():
        if is_blank(r["area_band"]) and is_blank(r["band_order"]):
            continue

        mapping_row = excel_row_number(idx)
        area_band = clean_area_band(r["area_band"], r.get("low_value"), r.get("high_value"))
        if area_band is None:
            errors.append(error_record("missing_area_band", sheet=SHEET_AREA_BANDS, mapping_row=mapping_row))
            continue

        band_order = coerce_int(r["band_order"], sheet=SHEET_AREA_BANDS, mapping_row=mapping_row, column="band_order", errors=errors)
        if band_order is None:
            continue

        is_none_band = coerce_boolish(
            r["is_none_band"],
            default=(area_band.lower() == "none"),
            sheet=SHEET_AREA_BANDS,
            mapping_row=mapping_row,
            column="is_none_band",
            errors=errors,
        )
        is_open_ended = coerce_boolish(
            r["is_open_ended"],
            default=area_band.lower().startswith("over "),
            sheet=SHEET_AREA_BANDS,
            mapping_row=mapping_row,
            column="is_open_ended",
            errors=errors,
        )

        rows.append(AreaBandRow(
            mapping_row=mapping_row,
            area_band=area_band,
            band_order=band_order,
            is_none_band=is_none_band,
            is_open_ended=is_open_ended,
            notes=clean_optional_text(r["notes"]),
        ))

    return rows


# -----------------------------------------------------------------------------
# Validation
# -----------------------------------------------------------------------------

def validate_destination_schema(conn: sqlite3.Connection, errors: list[dict[str, Any]]) -> None:
    existing = list_tables(conn)
    required = {
        TABLE_SOURCES,
        TABLE_INGEST_LOG,
        TABLE_WARNINGS,
        TABLE_DWELLINGS,
        TABLE_FIRE_CAT,
        TABLE_ITEMS,
        TABLE_ITEM_INFERENCE,
        TABLE_ROOMS,
        TABLE_AREA_BANDS,
        TABLE_INVENTORY_SNAPSHOT,
        TABLE_INVENTORY_ITEMS,
        TABLE_INVENTORY_ROOMS,
        TABLE_INVENTORY_DWELLINGS,
    }
    for table in sorted(required - existing):
        errors.append(error_record(
            "missing_destination_table",
            table=table,
            detail="Run or update scripts.fire.init_fire_db first.",
        ))


def validate_warning_catalogue(rows: list[WarningRow], errors: list[dict[str, Any]]) -> None:
    seen: dict[str, int] = {}
    for row in rows:
        if row.warning_type in seen:
            errors.append(error_record(
                "duplicate_warning_type",
                sheet=SHEET_WARNINGS,
                mapping_row=row.mapping_row,
                previous_mapping_row=seen[row.warning_type],
                warning_type=row.warning_type,
            ))
        seen[row.warning_type] = row.mapping_row


def validate_warning_references(
    rows: list[Any],
    valid_warning_types: set[str],
    sheet: str,
    errors: list[dict[str, Any]],
) -> None:
    for row in rows:
        warning_field = getattr(row, "warning_type", None)
        for warning_type in split_warning_types(warning_field):
            if warning_type not in valid_warning_types:
                errors.append(error_record(
                    "unknown_warning_type",
                    sheet=sheet,
                    mapping_row=getattr(row, "mapping_row", None),
                    warning_type=warning_type,
                ))


def validate_dwellings(rows: list[DwellingRow], errors: list[dict[str, Any]]) -> None:
    seen: dict[str, int] = {}
    for row in rows:
        if row.fris_dwelling_naming in seen:
            errors.append(error_record(
                "duplicate_dwelling_mapping",
                sheet=SHEET_DWELLINGS,
                mapping_row=row.mapping_row,
                previous_mapping_row=seen[row.fris_dwelling_naming],
                fris_dwelling_naming=row.fris_dwelling_naming,
            ))
        seen[row.fris_dwelling_naming] = row.mapping_row

        if row.occupancy_override is not None and row.occupancy_override not in ALLOWED_OCCUPANCY:
            errors.append(error_record(
                "invalid_occupancy_override",
                sheet=SHEET_DWELLINGS,
                mapping_row=row.mapping_row,
                occupancy_override=row.occupancy_override,
                allowed=sorted(ALLOWED_OCCUPANCY),
            ))


def validate_fire_cat(rows: list[FireCategoryRow], errors: list[dict[str, Any]]) -> None:
    seen: dict[str, int] = {}
    for row in rows:
        if row.fris_fire_categories in seen:
            errors.append(error_record(
                "duplicate_fire_cat_mapping",
                sheet=SHEET_FIRE_CAT,
                mapping_row=row.mapping_row,
                previous_mapping_row=seen[row.fris_fire_categories],
                fris_fire_categories=row.fris_fire_categories,
            ))
        seen[row.fris_fire_categories] = row.mapping_row

        if row.fire_spread_category not in ALLOWED_FIRE_SPREAD_CATEGORIES:
            errors.append(error_record(
                "invalid_fire_spread_category",
                sheet=SHEET_FIRE_CAT,
                mapping_row=row.mapping_row,
                fire_spread_category=row.fire_spread_category,
                allowed=sorted(ALLOWED_FIRE_SPREAD_CATEGORIES),
            ))

        if row.conditional_warning and row.warning_type is None:
            errors.append(error_record(
                "conditional_warning_without_warning_type",
                sheet=SHEET_FIRE_CAT,
                mapping_row=row.mapping_row,
                fris_fire_categories=row.fris_fire_categories,
            ))


def validate_items(rows: list[ItemRow], item_names: set[str], errors: list[dict[str, Any]]) -> None:
    seen: dict[str, int] = {}
    for row in rows:
        if row.ignition_source_all in seen:
            errors.append(error_record(
                "duplicate_item_mapping",
                sheet=SHEET_ITEMS,
                mapping_row=row.mapping_row,
                previous_mapping_row=seen[row.ignition_source_all],
                ignition_source_all=row.ignition_source_all,
            ))
        seen[row.ignition_source_all] = row.mapping_row

        if row.single_item_status not in ALLOWED_SINGLE_ITEM_STATUS:
            errors.append(error_record(
                "invalid_single_item_status",
                sheet=SHEET_ITEMS,
                mapping_row=row.mapping_row,
                ignition_source_all=row.ignition_source_all,
                single_item_status=row.single_item_status,
                allowed=sorted(ALLOWED_SINGLE_ITEM_STATUS),
            ))
            continue

        if row.single_item_status in MODEL_READY_SINGLE_ITEM_STATUS:
            if row.item_combusted is None:
                errors.append(error_record(
                    "missing_item_combusted",
                    sheet=SHEET_ITEMS,
                    mapping_row=row.mapping_row,
                    ignition_source_all=row.ignition_source_all,
                    single_item_status=row.single_item_status,
                ))
            elif row.item_combusted not in item_names:
                errors.append(error_record(
                    "item_combusted_not_in_inventory_snapshot",
                    sheet=SHEET_ITEMS,
                    mapping_row=row.mapping_row,
                    ignition_source_all=row.ignition_source_all,
                    item_combusted=row.item_combusted,
                ))
        else:
            if row.item_combusted is not None:
                errors.append(error_record(
                    "item_combusted_should_be_blank",
                    sheet=SHEET_ITEMS,
                    mapping_row=row.mapping_row,
                    ignition_source_all=row.ignition_source_all,
                    single_item_status=row.single_item_status,
                    item_combusted=row.item_combusted,
                ))


def validate_item_inference(
    rows: list[ItemInferenceRow],
    item_names: set[str],
    room_types: set[str],
    errors: list[dict[str, Any]],
) -> None:
    seen: dict[tuple[str, str, str, str, str], int] = {}
    for row in rows:
        key = (
            row.ignition_category or "",
            row.ignition_source,
            row.fire_spread_category,
            row.room_type or "",
            row.item_first_ignited or "",
        )
        if key in seen:
            errors.append(error_record(
                "duplicate_item_inference_mapping",
                sheet=SHEET_ITEM_INFERENCE,
                mapping_row=row.mapping_row,
                previous_mapping_row=seen[key],
                key=key,
            ))
        seen[key] = row.mapping_row

        if row.fire_spread_category != "single_item":
            errors.append(error_record(
                "item_inference_fire_spread_not_single_item",
                sheet=SHEET_ITEM_INFERENCE,
                mapping_row=row.mapping_row,
                fire_spread_category=row.fire_spread_category,
            ))

        if row.item_combusted not in item_names:
            errors.append(error_record(
                "item_inference_item_not_in_inventory_snapshot",
                sheet=SHEET_ITEM_INFERENCE,
                mapping_row=row.mapping_row,
                item_combusted=row.item_combusted,
            ))

        if row.room_type is not None and row.room_type not in room_types:
            errors.append(error_record(
                "item_inference_room_type_not_in_inventory_snapshot",
                sheet=SHEET_ITEM_INFERENCE,
                mapping_row=row.mapping_row,
                room_type=row.room_type,
            ))


def validate_rooms(rows: list[RoomRow], room_types: set[str], errors: list[dict[str, Any]]) -> None:
    seen: dict[str, int] = {}
    for row in rows:
        if row.fire_start_location in seen:
            errors.append(error_record(
                "duplicate_room_mapping",
                sheet=SHEET_ROOMS,
                mapping_row=row.mapping_row,
                previous_mapping_row=seen[row.fire_start_location],
                fire_start_location=row.fire_start_location,
            ))
        seen[row.fire_start_location] = row.mapping_row

        if row.room_type is not None and row.room_type not in room_types:
            errors.append(error_record(
                "room_type_not_in_inventory_snapshot",
                sheet=SHEET_ROOMS,
                mapping_row=row.mapping_row,
                fire_start_location=row.fire_start_location,
                room_type=row.room_type,
            ))


def validate_area_bands(rows: list[AreaBandRow], errors: list[dict[str, Any]]) -> None:
    seen_bands: dict[str, int] = {}
    seen_orders: dict[int, int] = {}
    n_none = 0

    for row in rows:
        if row.area_band in seen_bands:
            errors.append(error_record(
                "duplicate_area_band",
                sheet=SHEET_AREA_BANDS,
                mapping_row=row.mapping_row,
                previous_mapping_row=seen_bands[row.area_band],
                area_band=row.area_band,
            ))
        seen_bands[row.area_band] = row.mapping_row

        if row.band_order in seen_orders:
            errors.append(error_record(
                "duplicate_area_band_order",
                sheet=SHEET_AREA_BANDS,
                mapping_row=row.mapping_row,
                previous_mapping_row=seen_orders[row.band_order],
                band_order=row.band_order,
            ))
        seen_orders[row.band_order] = row.mapping_row

        if row.band_order < 0:
            errors.append(error_record(
                "negative_area_band_order",
                sheet=SHEET_AREA_BANDS,
                mapping_row=row.mapping_row,
                band_order=row.band_order,
            ))

        if row.is_none_band:
            n_none += 1

    if rows and n_none != 1:
        errors.append(error_record(
            "invalid_number_of_none_area_bands",
            sheet=SHEET_AREA_BANDS,
            count=n_none,
            detail="Exactly one area band should be marked as is_none_band.",
        ))


# -----------------------------------------------------------------------------
# Database read/write helpers
# -----------------------------------------------------------------------------

def get_current_inventory_snapshot_id(conn: sqlite3.Connection, errors: list[dict[str, Any]]) -> int | None:
    row = conn.execute(
        """
        SELECT inventory_snapshot_id
        FROM inventory_snapshot
        ORDER BY inventory_snapshot_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        errors.append(error_record(
            "missing_inventory_snapshot",
            detail="Run scripts.fire.inventory_snapshot before ingesting fire event mappings.",
        ))
        return None
    return int(row["inventory_snapshot_id"])


def load_current_item_names(conn: sqlite3.Connection, inventory_snapshot_id: int) -> set[str]:
    rows = conn.execute(
        """
        SELECT item_name
        FROM inventory_item_snapshot
        WHERE inventory_snapshot_id = ?
        """,
        (inventory_snapshot_id,),
    ).fetchall()
    return {str(row["item_name"]) for row in rows}


def load_current_room_types(conn: sqlite3.Connection, inventory_snapshot_id: int) -> set[str]:
    rows = conn.execute(
        """
        SELECT room_type
        FROM inventory_room_snapshot
        WHERE inventory_snapshot_id = ?
        """,
        (inventory_snapshot_id,),
    ).fetchall()
    return {str(row["room_type"]) for row in rows}


def delete_existing_mapping_rows(conn: sqlite3.Connection) -> dict[str, int]:
    deleted: dict[str, int] = {}
    for table in [
        TABLE_DWELLINGS,
        TABLE_FIRE_CAT,
        TABLE_ITEMS,
        TABLE_ITEM_INFERENCE,
        TABLE_ROOMS,
        TABLE_AREA_BANDS,
        TABLE_WARNINGS,
    ]:
        cur = conn.execute(f"DELETE FROM {table};")
        deleted[table] = cur.rowcount if cur.rowcount != -1 else 0
    return deleted


def insert_source_row(conn: sqlite3.Connection, source_id: str, xlsx_path: Path) -> None:
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
            xlsx_path.name,
            str(xlsx_path),
            None,
            None,
            utc_now_iso(),
            "Controlled fire event mapping workbook.",
        ),
    )


def insert_ingest_log(
    conn: sqlite3.Connection,
    *,
    source_id: str | None,
    status: str,
    message: str,
    started_utc: str,
    finished_utc: str,
    rows_inserted: int | None,
    rows_deleted: int | None,
) -> None:
    conn.execute(
        """
        INSERT INTO ingest_log (
            source_id,
            data_source_type,
            action,
            status,
            message,
            started_utc,
            finished_utc,
            rows_inserted,
            rows_deleted
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_id,
            SOURCE_TYPE,
            "ingest",
            status,
            message,
            started_utc,
            finished_utc,
            rows_inserted,
            rows_deleted,
        ),
    )


def insert_warning_rows(conn: sqlite3.Connection, rows: list[WarningRow]) -> None:
    conn.executemany(
        """
        INSERT INTO fire_event_mapping_warnings (
            mapping_row,
            warning_category,
            warning_type,
            warning_text,
            notes
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [(r.mapping_row, r.warning_category, r.warning_type, r.warning_text, r.notes) for r in rows],
    )


def insert_dwelling_rows(conn: sqlite3.Connection, rows: list[DwellingRow]) -> None:
    conn.executemany(
        """
        INSERT INTO fire_event_mapping_dwellings (
            mapping_row,
            fris_dwelling_naming,
            dwelling_type,
            dwelling_type_proxy,
            occupancy_override,
            omit_from_model,
            warning_type,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r.mapping_row,
                r.fris_dwelling_naming,
                r.dwelling_type,
                r.dwelling_type_proxy,
                r.occupancy_override,
                r.omit_from_model,
                r.warning_type,
                r.notes,
            )
            for r in rows
        ],
    )


def insert_fire_cat_rows(conn: sqlite3.Connection, rows: list[FireCategoryRow]) -> None:
    conn.executemany(
        """
        INSERT INTO fire_event_mapping_fire_cat (
            mapping_row,
            fris_fire_categories,
            fire_spread_category,
            omit_from_model,
            occupancy_dependent,
            warning_type,
            conditional_warning,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r.mapping_row,
                r.fris_fire_categories,
                r.fire_spread_category,
                r.omit_from_model,
                r.occupancy_dependent,
                r.warning_type,
                r.conditional_warning,
                r.notes,
            )
            for r in rows
        ],
    )


def insert_item_rows(conn: sqlite3.Connection, rows: list[ItemRow]) -> None:
    conn.executemany(
        """
        INSERT INTO fire_event_mapping_items (
            mapping_row,
            ignition_source_all,
            ignition_source_category_override,
            ignition_source_override,
            single_item_status,
            item_combusted,
            warning_type,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r.mapping_row,
                r.ignition_source_all,
                r.ignition_source_category_override,
                r.ignition_source_override,
                r.single_item_status,
                r.item_combusted,
                r.warning_type,
                r.notes,
            )
            for r in rows
        ],
    )


def insert_item_inference_rows(conn: sqlite3.Connection, rows: list[ItemInferenceRow]) -> None:
    conn.executemany(
        """
        INSERT INTO fire_event_mapping_item_inference (
            mapping_row,
            ignition_category,
            ignition_source,
            fire_spread_category,
            room_type,
            item_first_ignited,
            item_combusted,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                r.mapping_row,
                r.ignition_category,
                r.ignition_source,
                r.fire_spread_category,
                r.room_type,
                r.item_first_ignited,
                r.item_combusted,
                r.notes,
            )
            for r in rows
        ],
    )


def insert_room_rows(conn: sqlite3.Connection, rows: list[RoomRow]) -> None:
    conn.executemany(
        """
        INSERT INTO fire_event_mapping_rooms (
            mapping_row,
            fire_start_location,
            room_type,
            warning_type,
            notes
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [(r.mapping_row, r.fire_start_location, r.room_type, r.warning_type, r.notes) for r in rows],
    )


def insert_area_band_rows(conn: sqlite3.Connection, rows: list[AreaBandRow]) -> None:
    conn.executemany(
        """
        INSERT INTO fire_event_mapping_area_bands (
            mapping_row,
            area_band,
            band_order,
            is_none_band,
            is_open_ended,
            notes
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        [(r.mapping_row, r.area_band, r.band_order, r.is_none_band, r.is_open_ended, r.notes) for r in rows],
    )


def count_mapping_rows(conn_or_path: sqlite3.Connection | str | Path) -> dict[str, int]:
    close_after = False
    if isinstance(conn_or_path, sqlite3.Connection):
        conn = conn_or_path
    else:
        conn = sqlite3.connect(str(conn_or_path))
        close_after = True

    try:
        cur = conn.cursor()
        out: dict[str, int] = {}
        total = 0
        for table in MAPPING_TABLES:
            n = int(cur.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0])
            out[f"rows_{table}"] = n
            total += n
        out["rows_total"] = total
        return out
    finally:
        if close_after:
            conn.close()


# -----------------------------------------------------------------------------
# Data cleaning helpers
# -----------------------------------------------------------------------------

def read_sheet(xlsx_path: Path, sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(xlsx_path, sheet_name=sheet_name, engine="openpyxl", dtype=object)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def rename_aliases(df: pd.DataFrame, aliases: dict[str, str]) -> None:
    rename: dict[str, str] = {}
    for old, new in aliases.items():
        if old in df.columns and new not in df.columns:
            rename[old] = new
    if rename:
        df.rename(columns=rename, inplace=True)


def require_columns(df: pd.DataFrame, required: list[str], sheet: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Sheet '{sheet}' missing required columns: {missing}. Found: {list(df.columns)}"
        )


def excel_row_number(df_index: Any) -> int:
    return int(df_index) + 2


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except Exception:
        pass
    return str(value).strip() == ""


def clean_text(value: Any) -> str:
    return str(value).strip()


def clean_optional_text(value: Any) -> str | None:
    if is_blank(value):
        return None
    return str(value).strip()


def clean_key(value: Any) -> str:
    text = str(value).strip().lower()
    text = text.replace(" ", "_")
    return text


def clean_key_or_none(value: Any) -> str | None:
    if is_blank(value):
        return None
    return clean_key(value)


def clean_warning_type_field(value: Any) -> str | None:
    if is_blank(value):
        return None
    parts = [clean_key(p) for p in str(value).split(";") if p.strip()]
    return "; ".join(parts) if parts else None


def split_warning_types(value: str | None) -> list[str]:
    if value is None:
        return []
    return [clean_key(part) for part in value.split(";") if part.strip()]


def clean_area_band(value: Any, low_value: Any = None, high_value: Any = None) -> str | None:
    if is_blank(value):
        # The workbook sometimes represents the "None" band as a blank label
        # with low/high values of 0. Treat that as the valid no-damage band.
        try:
            low = float(low_value)
            high = float(high_value)
            if low == 0 and high == 0:
                return "None"
        except Exception:
            pass
        return None

    text = str(value).strip()
    # Normalise common dash variants used by Excel/Word auto-formatting.
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")
    # Remove spaces around hyphens in numeric ranges, e.g. "6 - 10" -> "6-10".
    text = "-".join(part.strip() for part in text.split("-"))
    return text


def coerce_boolish(
    value: Any,
    *,
    default: bool,
    sheet: str,
    mapping_row: int,
    column: str,
    errors: list[dict[str, Any]],
) -> int:
    if is_blank(value):
        return int(default)

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if value in (0, 1):
            return int(value)

    text = str(value).strip().lower()
    if text in {"true", "yes", "y", "1"}:
        return 1
    if text in {"false", "no", "n", "0"}:
        return 0

    errors.append(error_record(
        "invalid_boolean_value",
        sheet=sheet,
        mapping_row=mapping_row,
        column=column,
        raw_value=repr(value),
        allowed=["TRUE", "FALSE", "yes", "no", 1, 0],
    ))
    return int(default)


def coerce_int(
    value: Any,
    *,
    sheet: str,
    mapping_row: int,
    column: str,
    errors: list[dict[str, Any]],
) -> int | None:
    if is_blank(value):
        errors.append(error_record("missing_integer_value", sheet=sheet, mapping_row=mapping_row, column=column))
        return None
    try:
        numeric = float(value)
        if not numeric.is_integer():
            raise ValueError("not an integer")
        return int(numeric)
    except Exception:
        errors.append(error_record(
            "invalid_integer_value",
            sheet=sheet,
            mapping_row=mapping_row,
            column=column,
            raw_value=repr(value),
        ))
        return None


def error_record(error_type: str, **kwargs: Any) -> dict[str, Any]:
    out = {"type": error_type}
    out.update(kwargs)
    return out


def format_validation_errors(errors: list[dict[str, Any]]) -> str:
    lines = ["fire_event_mappings.xlsm contains blocking validation errors:"]
    for err in errors[:50]:
        lines.append(f"  - {err}")
    if len(errors) > 50:
        lines.append(f"  ... {len(errors) - 50} more error(s)")
    return "\n".join(lines)


def utc_now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def list_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        """
    ).fetchall()
    return {str(row["name"]) for row in rows}
