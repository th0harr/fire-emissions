# scripts/fire/fire_event_resolver.py
from __future__ import annotations

import math
import re
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


# -----------------------------------------------------------------------------
# Optional project imports
# -----------------------------------------------------------------------------

# Keep DB connection handling deliberately local in this module.
#
# Earlier versions imported ``db_connect`` from a project helper.  That made the
# script slightly more compact, but it also made debugging harder because a
# path-like database argument could be reinterpreted by the helper.  The fire
# event build already resolves ``--profile`` + ``--db`` to one explicit SQLite
# path before it reaches this module, so the safest thing to do here is simply
# open that exact file.
def db_connect(db_path: str | Path) -> sqlite3.Connection:
    return sqlite3.connect(str(db_path))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# -----------------------------------------------------------------------------
# Table names
# -----------------------------------------------------------------------------

# FRIS staging table created by the bulk FRIS ingest.
TABLE_FRIS_STAGING = "input_bulk_fris_events"

# Model-facing output tables.
TABLE_FIRE_EVENTS = "fire_events"
TABLE_FIRE_EVENT_WARNINGS = "fire_event_warnings"

# New route-specific mapping tables ingested from fire_event_mappings.xlsm.
# The code treats these as controlled configuration, not ordinary optional data.
TABLE_MAPPING_FIRE_CAT = "fire_event_mapping_fire_cat"
TABLE_MAPPING_DWELLINGS = "fire_event_mapping_dwellings"
TABLE_MAPPING_ROOMS = "fire_event_mapping_rooms"
TABLE_MAPPING_ITEMS = "fire_event_mapping_items"
TABLE_MAPPING_ITEM_INFERENCE = "fire_event_mapping_item_inference"
TABLE_MAPPING_AREA_BANDS = "fire_event_mapping_area_bands"
TABLE_MAPPING_WARNINGS = "fire_event_mapping_warnings"


# -----------------------------------------------------------------------------
# Controlled values
# -----------------------------------------------------------------------------

INPUT_TYPE_FRIS = "fris"

# These values mean that a raw FRIS field is unusable.
#
# Important:
#   Do not include the text value "None" here.
#   FRIS uses "None" as a valid area-band category, meaning zero damage area.
#   This must not be collapsed into NULL/missing.
RAW_MISSING_TEXT = {
    "",
    "null",
    "nan",
    "na",
    "n/a",
}

# This is the valid FRIS area-band label for zero area.
AREA_BAND_NONE = "None"

# Current model-facing fire-spread categories.
#
# `none`, `roof`, and `unspecified` are included because they may appear as
# resolver categories from FRIS mapping.  The later emissions model can decide
# whether they are zero-output records or omitted records, but the resolver must
# be able to name them explicitly.
VALID_FIRE_SPREAD_CATEGORIES = {
    "none",
    "heat_smoke",
    "heat_smoke_damage_only",
    "single_item",
    "within_room",
    "multiple_rooms",
    "entire_dwelling",
    "roof",
    "unspecified",
}

# These categories need a model room for the current inventory lookup route.
#
# `entire_dwelling` can retain room_of_origin as metadata, but it does not need
# a room to calculate whole-dwelling stock.  `heat_smoke` may later use room
# information for replacement allocation, but at this stage it is not required
# to validate the direct combustion pathway.
FIRE_CATEGORIES_REQUIRING_ROOM = {
    "single_item",
    "within_room",
    "multiple_rooms",
}

# Warning severities allowed in fire_event_mappings.xlsm.
#
# The spreadsheet can make row-level omissions explicit, but core safety checks
# are still hard-coded.  For example, an unmapped new FRIS category is always a
# blocking build error even if somebody forgets to put a blocking warning in the
# spreadsheet.
VALID_WARNING_SEVERITIES = {
    "info",
    "warning",
    "omit_row",
    "blocking",
}

# Current single-item statuses.
VALID_SINGLE_ITEM_STATUSES = {
    "direct_inventory_item",
    "proxy_inventory_item",
    "invalid_single_item",
    "conditionally_inferred_item",
}

MODELLED_SINGLE_ITEM_STATUSES = {
    "direct_inventory_item",
    "proxy_inventory_item",
    "conditionally_inferred_item",
}


# Fallback area-band order.
#
# The preferred source is fire_event_mapping_area_bands, because this makes the
# traffic-light assumptions visible in the mapping workbook.  The fallback is
# only used for explicit unit tests / isolated scripts if the caller asks for it.
DEFAULT_AREA_BAND_ORDER = [
    AREA_BAND_NONE,
    "Up to 5",
    "6-10",
    "11-20",
    "21-50",
    "51-100",
    "101-200",
    "201-500",
    "501-1,000",
    "1,001-2,000",
    "2,001-5,000",
    "5,001-10,000",
    "Over 10,000",
]


# -----------------------------------------------------------------------------
# Column aliases
# -----------------------------------------------------------------------------

# The raw FRIS ingest may preserve the original FRIS column names, or it may
# snake-case them during staging.  These aliases make the resolver tolerant to
# either version without hiding the expected fields.
COL_ALIASES = {
    "incident_id": ["incident_id", "Incident_Id", "Incident ID"],
    "source_id": ["source_id"],
    "fiscal_year": ["fiscal_yr", "Fiscal_Year", "Fiscal_yr", "Fiscal Yr"],
    "property_type_3": ["property_type_3", "Property_Type_3", "Property Type 3"],
    "extent_of_damage": ["extent_of_damage", "Extent_of_Damage", "Extent of Damage"],
    "fire_start_location": ["fire_start_location", "Fire_Start_Location", "Fire Start Location"],
    "heat_smoke_damage_only": [
        "heat_smoke_damage_only",
        "Heat_Smoke_Damage_Only",
        "Heat Smoke Damage Only",
    ],
    "building_fire_damage_area": [
        "building_fire_damage_area",
        "Building_Fire_Damage_Area",
        "Building Fire Damage Area",
    ],
    "building_total_damage_area": [
        "building_total_damage_area",
        "Building_Total_Damage_Area",
        "Building Total Damage Area",
    ],
    "building_room_origin_size": [
        "building_room_origin_size",
        "Building_Room_Origin_Size",
        "Building Room Origin Size",
    ],
    "building_floor_origin_size": [
        "building_floor_origin_size",
        "Building_Floor_Origin_Size",
        "Building Floor Origin Size",
    ],
    "ignition_source_all": [
        "ignition_source_all",
        "Ignition_Source_All",
        "Ignition Source All",
    ],
    "item_first_ignited": [
        "item_first_ignited",
        "Item_First_Ignited",
        "Item First Ignited",
    ],
    "item_causing_spread": [
        "item_causing_spread",
        "Item_Causing_Spread",
        "Item Causing Spread",
    ],
    "fire_size_on_arrival": [
        "fire_size_on_arrival",
        "Fire_Size_On_Arrival",
        "Fire Size On Arrival",
    ],
    "rapid_fire_growth": [
        "rapid_fire_growth",
        "Rapid_Fire_Growth",
        "Rapid Fire Growth",
    ],
    "distance_to_adjoining_property": [
        "distance_to_adjoining_property",
        "Distance_to_Adjoining_Property",
        "Distance to Adjoining Property",
    ],
}


# -----------------------------------------------------------------------------
# Error types
# -----------------------------------------------------------------------------

class BlockingResolutionError(RuntimeError):
    """
    Error that should stop the whole FRIS build.

    This is used when the input/configuration is incomplete or unsafe, rather
    than when one incident row has missing data.

    Examples
    --------
    - A present Extent_of_Damage category is not in fire_event_mapping_fire_cat.
    - A present Fire_Start_Location category is not in fire_event_mapping_rooms.
    - The mapping workbook contains duplicate keys.
    """


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class WarningTemplate:
    """
    One controlled warning template from fire_event_mapping_warnings.
    """

    warning_type: str
    warning_text: str
    warning_severity: str = "warning"
    warning_category: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class FireEventWarning:
    """
    One warning row to be written to fire_event_warnings.

    The table insert is schema-adaptive, so not every field has to exist in the
    current database yet.  Extra fields are ignored if the table does not have a
    matching column.
    """

    incident_id: str
    source_id: Optional[str]
    input_type: str
    warning_type: str
    warning_severity: str
    warning_category: Optional[str]
    warning_text: str
    fire_parameter: Optional[str] = None
    raw_value: Optional[str] = None
    resolved_value: Optional[str] = None
    created_at_utc: str = field(default_factory=utc_now_iso)

    def to_insert_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PreparedFireEvent:
    """
    One model-facing event row produced by the resolver.

    This is deliberately slightly wider than the current fire_events table.
    During insertion we only write the columns that exist in the DB.  This lets
    the code and schema be updated in small steps without losing the intended
    event shape.
    """

    incident_id: str
    source_id: Optional[str]
    input_type: str = INPUT_TYPE_FRIS
    inventory_snapshot_id: Optional[str] = None

    fiscal_year_start: Optional[int] = None
    fiscal_year_end: Optional[int] = None

    property_type_input: Optional[str] = None
    dwelling_type_input: Optional[str] = None
    dwelling_type: Optional[str] = None
    dwelling_type_proxy: Optional[str] = None
    dwelling_type_for_model: Optional[str] = None
    occupancy: Optional[str] = None

    heat_smoke_damage_only_input: Optional[str] = None
    heat_smoke_damage_only: Optional[str] = None

    fire_spread_category_input: Optional[str] = None
    fire_spread_category: Optional[str] = None

    room_of_origin_input: Optional[str] = None
    room_of_origin: Optional[str] = None
    room_of_origin_proxy: Optional[str] = None

    building_fire_damage_area_input: Optional[str] = None
    building_total_damage_area_input: Optional[str] = None
    building_total_damage_area_for_model: Optional[str] = None
    fire_damage_band_index: Optional[int] = None
    total_damage_band_index: Optional[int] = None
    total_damage_tier_difference: Optional[int] = None

    room_of_origin_size_input: Optional[str] = None
    origin_floor_size_input: Optional[str] = None
    dwelling_size_input: Optional[str] = None

    ignition_source_all_input: Optional[str] = None
    ignition_source_category: Optional[str] = None
    ignition_source: Optional[str] = None
    item_first_ignited_input: Optional[str] = None
    item_causing_spread_input: Optional[str] = None
    fire_size_on_arrival_input: Optional[str] = None
    rapid_fire_growth_input: Optional[str] = None
    distance_to_adjoining_property_input: Optional[str] = None

    single_item_status: Optional[str] = None
    item_combusted: Optional[str] = None

    data_quality_status: str = "ok"
    suspicious_fields: Optional[str] = None
    omit_from_model: str = "no"
    omit_reason: Optional[str] = None
    resolution_notes: Optional[str] = None
    created_at_utc: str = field(default_factory=utc_now_iso)

    def mark_omitted(self, reason: str, suspicious_field: Optional[str] = None) -> None:
        """
        Mark this prepared row as omitted from the model.

        I keep omitted rows as PreparedFireEvent objects so that the caller can
        decide whether to insert them into fire_events or only write warnings.
        This is useful while the schema is still moving.
        """
        self.data_quality_status = "omit"
        self.omit_from_model = "yes"
        self.omit_reason = reason
        if suspicious_field:
            self.suspicious_fields = append_delimited(self.suspicious_fields, suspicious_field)

    def add_note(self, note: str) -> None:
        self.resolution_notes = append_delimited(self.resolution_notes, note, delimiter=" | ")

    def to_insert_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MappingWorkbook:
    """
    In-memory representation of the event-resolution mapping tables.
    """

    fire_cat_by_extent: dict[str, dict[str, Any]]
    dwellings_by_property: dict[str, dict[str, Any]]
    rooms_by_location: dict[str, dict[str, Any]]
    area_band_index: dict[str, int]
    item_rows: list[dict[str, Any]]
    item_inference_rows: list[dict[str, Any]]
    warning_templates: dict[str, WarningTemplate]


@dataclass
class BuildSummary:
    """
    Small summary object returned by the FRIS preparation route.
    """

    rows_read: int = 0
    rows_prepared: int = 0
    rows_insertable: int = 0
    rows_omitted: int = 0
    warnings: int = 0
    blocking_checks_passed: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# -----------------------------------------------------------------------------
# Public mapping loader
# -----------------------------------------------------------------------------

def load_fire_event_mappings(conn: sqlite3.Connection) -> MappingWorkbook:
    """
    Load the controlled mapping tables used by the FRIS resolver.

    This function is intentionally strict.  The FRIS resolver should not silently
    guess how to interpret a new raw category.  A present-but-unmapped category
    usually means SFRS/FRIS has added a new option, or that the mapping workbook
    has been edited incompletely.
    """
    required_tables = {
        TABLE_MAPPING_FIRE_CAT,
        TABLE_MAPPING_DWELLINGS,
        TABLE_MAPPING_ROOMS,
        TABLE_MAPPING_ITEMS,
        TABLE_MAPPING_ITEM_INFERENCE,
        TABLE_MAPPING_AREA_BANDS,
        TABLE_MAPPING_WARNINGS,
    }

    existing = list_tables(conn)
    missing = sorted(required_tables - existing)
    if missing:
        raise BlockingResolutionError(
            "fire_db is missing required fire-event mapping table(s): "
            + ", ".join(missing)
            + ". Ingest fire_event_mappings.xlsm before building fire_events."
        )

    warning_templates = _load_warning_templates(conn)

    fire_cat_by_extent = _load_keyed_mapping(
    conn,
    table=TABLE_MAPPING_FIRE_CAT,
    key_aliases=["fris_fire_categories", "extent_of_damage", "input_value", "fris_extent_of_damage"],
    display_name="fire category mapping",
)

    dwellings_by_property = _load_keyed_mapping(
        conn,
        table=TABLE_MAPPING_DWELLINGS,
        key_aliases=["property_type_3", "fris_dwelling_naming", "dwelling_type_input"],
        display_name="dwelling mapping",
    )

    rooms_by_location = _load_keyed_mapping(
        conn,
        table=TABLE_MAPPING_ROOMS,
        key_aliases=["fire_start_location", "room_of_origin_input", "input_value"],
        display_name="room mapping",
    )

    area_band_index = _load_area_band_index(conn)

    item_rows = fetch_table_rows(conn, TABLE_MAPPING_ITEMS)
    _validate_warning_types_used(
        mapping_rows=(
            list(fire_cat_by_extent.values())
            + list(dwellings_by_property.values())
            + list(rooms_by_location.values())
            + item_rows
        ),
        warning_templates=warning_templates,
    )

    item_inference_rows = fetch_table_rows(conn, TABLE_MAPPING_ITEM_INFERENCE)

    return MappingWorkbook(
        fire_cat_by_extent=fire_cat_by_extent,
        dwellings_by_property=dwellings_by_property,
        rooms_by_location=rooms_by_location,
        area_band_index=area_band_index,
        item_rows=item_rows,
        item_inference_rows=item_inference_rows,
        warning_templates=warning_templates,
    )


def validate_mapping_coverage_for_fris(
    conn: sqlite3.Connection,
    mappings: MappingWorkbook,
) -> None:
    """
    Validate that all present raw FRIS categories are covered by mappings.

    Missing values in individual rows are not handled here.  They are row-level
    data-quality problems and will omit those rows later.

    Present values that are absent from a required mapping table are different:
    they imply that the configuration is incomplete.  Those should stop the
    build so that we do not accidentally miss an important category.
    """
    if TABLE_FRIS_STAGING not in list_tables(conn):
        raise BlockingResolutionError(
            f"fire_db is missing FRIS staging table: {TABLE_FRIS_STAGING}."
        )

    staging_cols = table_columns(conn, TABLE_FRIS_STAGING)

    checks = [
        (
            "property_type_3",
            mappings.dwellings_by_property,
            "fire_event_mapping_dwellings",
        ),
        (
            "extent_of_damage",
            mappings.fire_cat_by_extent,
            "fire_event_mapping_fire_cat",
        ),
        (
            "fire_start_location",
            mappings.rooms_by_location,
            "fire_event_mapping_rooms",
        ),
        (
            "building_fire_damage_area",
            mappings.area_band_index,
            "fire_event_mapping_area_bands",
        ),
        (
            "building_total_damage_area",
            mappings.area_band_index,
            "fire_event_mapping_area_bands",
        ),
    ]

    missing_messages: list[str] = []

    for canonical_name, mapping_lookup, mapping_table in checks:
        actual_col = first_existing_column(staging_cols, COL_ALIASES[canonical_name])
        if actual_col is None:
            raise BlockingResolutionError(
                f"FRIS staging table is missing required column for {canonical_name}. "
                f"Accepted aliases: {COL_ALIASES[canonical_name]}"
            )

        values = distinct_column_values(conn, TABLE_FRIS_STAGING, actual_col)
        present_keys = {
            normalise_lookup_key(v)
            for v in values
            if normalise_raw_value(v) is not None
        }

        missing_keys = sorted(k for k in present_keys if k not in mapping_lookup)
        if missing_keys:
            examples = ", ".join(missing_keys[:20])
            more = "" if len(missing_keys) <= 20 else f" ... +{len(missing_keys) - 20} more"
            missing_messages.append(
                f"{canonical_name} has present value(s) missing from {mapping_table}: "
                f"{examples}{more}"
            )

    if missing_messages:
        raise BlockingResolutionError(
            "Blocking mapping coverage error.\n" + "\n".join(missing_messages)
        )


# -----------------------------------------------------------------------------
# Warning helpers
# -----------------------------------------------------------------------------

def append_warning(
    warnings: list[FireEventWarning],
    *,
    mappings: MappingWorkbook,
    incident_id: str,
    source_id: Optional[str],
    warning_type: str,
    input_type: str = INPUT_TYPE_FRIS,
    fire_parameter: Optional[str] = None,
    raw_value: Optional[Any] = None,
    resolved_value: Optional[Any] = None,
    template_values: Optional[dict[str, Any]] = None,
    fallback_text: Optional[str] = None,
    fallback_severity: str = "warning",
    fallback_category: Optional[str] = None,
) -> None:
    """
    Append one structured warning.

    Warning text normally comes from fire_event_mapping_warnings.  A fallback is
    allowed for hard-coded checks that should still work before the warning
    catalogue has been fully populated.
    """
    template = mappings.warning_templates.get(warning_type)

    if template:
        text_template = template.warning_text
        severity = template.warning_severity
        category = template.warning_category
    else:
        text_template = fallback_text or warning_type
        severity = fallback_severity
        category = fallback_category

    if severity not in VALID_WARNING_SEVERITIES:
        raise BlockingResolutionError(
            f"Invalid warning_severity '{severity}' for warning_type '{warning_type}'."
        )

    values = dict(template_values or {})
    values.setdefault("warning_type", warning_type)
    values.setdefault("fire_parameter", fire_parameter)
    values.setdefault("raw_value", raw_value)
    values.setdefault("resolved_value", resolved_value)

    try:
        warning_text = text_template.format(**values)
    except Exception:
        # Do not let a small placeholder mistake hide the warning entirely.
        # This still preserves the template text so the workbook can be fixed.
        warning_text = text_template

    warnings.append(
        FireEventWarning(
            incident_id=incident_id,
            source_id=source_id,
            input_type=input_type,
            warning_type=warning_type,
            warning_severity=severity,
            warning_category=category,
            warning_text=warning_text,
            fire_parameter=fire_parameter,
            raw_value=to_optional_str(raw_value),
            resolved_value=to_optional_str(resolved_value),
        )
    )


def mark_omit_with_warning(
    event: PreparedFireEvent,
    warnings: list[FireEventWarning],
    *,
    mappings: MappingWorkbook,
    warning_type: str,
    reason: str,
    fire_parameter: Optional[str] = None,
    raw_value: Optional[Any] = None,
    resolved_value: Optional[Any] = None,
    template_values: Optional[dict[str, Any]] = None,
    fallback_text: Optional[str] = None,
) -> None:
    """
    Mark an event as omitted and append a matching warning.
    """
    event.mark_omitted(reason=reason, suspicious_field=fire_parameter)
    append_warning(
        warnings,
        mappings=mappings,
        incident_id=event.incident_id,
        source_id=event.source_id,
        warning_type=warning_type,
        fire_parameter=fire_parameter,
        raw_value=raw_value,
        resolved_value=resolved_value,
        template_values=template_values,
        fallback_text=fallback_text,
        fallback_severity="omit_row",
    )


# -----------------------------------------------------------------------------
# Field normalisation
# -----------------------------------------------------------------------------

def normalise_raw_value(value: Any) -> Optional[str]:
    """
    Convert raw DB/Excel values to stripped text, or None if unusable.

    This is intentionally not the same as area-band parsing.  The raw text value
    "None" is preserved because it is a valid FRIS damage-area band.
    """
    if value is None:
        return None

    if isinstance(value, float) and math.isnan(value):
        return None

    text = str(value).strip()
    if text.lower() in RAW_MISSING_TEXT:
        return None

    return text


def normalise_heat_smoke_damage_only(value: Any) -> str:
    """
    Resolve heat_smoke_damage_only to exactly yes / no / NULL.

    FRIS probably stores this as a yes/no multiple-choice field, with NULL when
    the field is missed.  I still route unexpected values to NULL so that the
    row fails fast rather than being interpreted as no.
    """
    text = normalise_raw_value(value)
    if text is None:
        return "NULL"

    cleaned = text.strip().lower()
    if cleaned in {"yes", "y", "true", "1"}:
        return "yes"
    if cleaned in {"no", "n", "false", "0"}:
        return "no"

    return "NULL"


def normalise_lookup_key(value: Any) -> str:
    """
    Normalise a mapping key for dictionary lookup.

    This keeps the wording recognisable, but removes small Excel/FRIS formatting
    differences that should not create different categories.

    Raw FRIS area bands seem to use different range separators depending on the raw
    export, Excel, terminal encoding, or copy/paste route:

        6-10
        6 – 10
        6 ? 10
        6 � 10

    These should resolve to a simple hyphenenated range with no spaces.
    The range separator is only formatting; the model assumption is the ordered area-band category.
    """
    text = normalise_raw_value(value)
    if text is None:
        return ""

    # Standardise common unicode dash variants to a plain hyphen.
    text = re.sub(r"[\u2010\u2011\u2012\u2013\u2014\u2212]", "-", text)

    # If an en dash has already been mangled by encoding, it may arrive as a
    # literal '?' or the unicode replacement character '�'.  Only treat these as
    # range separators when they are between numeric-looking endpoints, so that
    # ordinary question marks elsewhere are not changed.
    text = re.sub(
        r"(?<=[0-9,])\s*[\?\uFFFD]\s*(?=[0-9,])",
        "-",
        text,
    )

    # Collapse spaces around normal hyphens so that "6 - 10" and "6-10" match.
    text = re.sub(r"\s*-\s*", "-", text)

    # Collapse any remaining repeated whitespace.
    text = re.sub(r"\s+", " ", text)

    return text.strip().lower()


def clean_code(value: Any) -> Optional[str]:
    """
    Clean a controlled code value from the mapping workbook.
    """
    text = normalise_raw_value(value)
    if text is None:
        return None
    return text.strip().lower().replace(" ", "_")


def parse_bool_like(value: Any, default: bool = False) -> bool:
    """
    Parse spreadsheet-style yes/no values.
    """
    text = normalise_raw_value(value)
    if text is None:
        return default

    cleaned = text.strip().lower()
    if cleaned in {"yes", "y", "true", "1"}:
        return True
    if cleaned in {"no", "n", "false", "0"}:
        return False
    return default


def to_optional_str(value: Any) -> Optional[str]:
    text = normalise_raw_value(value)
    return text


def append_delimited(
    existing: Optional[str],
    value: str,
    *,
    delimiter: str = "; ",
) -> str:
    """
    Append one value to a delimited text field without duplicating it.
    """
    if not value:
        return existing or ""

    if not existing:
        return value

    parts = [p.strip() for p in existing.split(delimiter) if p.strip()]
    if value not in parts:
        parts.append(value)
    return delimiter.join(parts)


def split_ignition_source_all(value: Any) -> tuple[Optional[str], Optional[str]]:
    """
    Split FRIS Ignition_Source_All into category and subcategory.

    The expected format is:
        Category: subcategory

    If the separator is absent, keep the full value as ignition_source and leave
    the category blank.  This preserves the raw information while making the
    likely mapping issue visible elsewhere.
    """
    text = normalise_raw_value(value)
    if text is None:
        return None, None

    if ":" not in text:
        return None, text

    left, right = text.split(":", 1)
    return left.strip() or None, right.strip() or None


def parse_fiscal_year(value: Any) -> tuple[Optional[int], Optional[int]]:
    """
    Parse a FRIS fiscal year value into start/end years.

    The resolver only needs light parsing here; detailed temporal analysis can
    remain downstream.
    """
    text = normalise_raw_value(value)
    if text is None:
        return None, None

    # Common values look like 2009/10 or 2009-10.
    match = re.search(r"(?P<start>20\d{2})", text)
    if not match:
        return None, None

    start = int(match.group("start"))
    return start, start + 1


def split_warning_types(value: Any) -> list[str]:
    """
    Split one or more warning_type values from a mapping-table cell.
    """
    raw_value = normalise_raw_value(value)

    if raw_value is None:
        return []

    warning_types: list[str] = []

    for part in raw_value.split(";"):
        warning_type = clean_code(part)

        if warning_type is not None and warning_type not in warning_types:
            warning_types.append(warning_type)

    return warning_types



# -----------------------------------------------------------------------------
# Conditional item inference
# -----------------------------------------------------------------------------

def _match_optional_context(
    *,
    rule_value: Any,
    event_value: Any,
) -> tuple[bool, int]:
    """
    Match one optional contextual inference field.

    A NULL / blank value in the mapping rule acts as a wildcard.
    A populated value must match the event value after lookup normalisation.

    Returns:
        (matched, specificity_score)

    specificity_score is 1 when the rule used a populated contextual value,
    and 0 when the rule used a wildcard.
    """
    rule_text = normalise_raw_value(rule_value)

    if rule_text is None:
        return True, 0

    if normalise_lookup_key(rule_text) == normalise_lookup_key(event_value):
        return True, 1

    return False, 0


def resolve_conditionally_inferred_item(
    *,
    event: PreparedFireEvent,
    mappings: MappingWorkbook,
) -> Optional[str]:
    """
    Resolve item_combusted for conditionally inferred single-item cases.

    These are cases where Ignition_Source_All alone is too broad, but
    Item_First_Ignited and/or room_of_origin can support a defensible proxy item.

    Matching rules:
        - ignition_source must match
        - fire_spread_category must match
        - ignition_category, room_type and item_first_ignited may either match
          exactly or be blank/NULL in the rule as wildcards

    If multiple top-ranked rules resolve to the same item_combusted, this is
    treated as unambiguous. If they resolve to different items, the mapping
    workbook is ambiguous and the build stops.
    """
    matches: list[tuple[int, str, dict[str, Any]]] = []

    for rule in mappings.item_inference_rows:
        # Required rule fields.
        if normalise_lookup_key(get_any(rule, ["ignition_source"])) != normalise_lookup_key(event.ignition_source):
            continue

        if normalise_lookup_key(get_any(rule, ["fire_spread_category"])) != normalise_lookup_key(event.fire_spread_category):
            continue

        # Optional contextual fields. Blank in the rule means wildcard.
        category_ok, category_score = _match_optional_context(
            rule_value=get_any(rule, ["ignition_category"]),
            event_value=event.ignition_source_category,
        )

        room_ok, room_score = _match_optional_context(
            rule_value=get_any(rule, ["room_type"]),
            event_value=event.room_of_origin,
        )

        first_item_ok, first_item_score = _match_optional_context(
            rule_value=get_any(rule, ["item_first_ignited"]),
            event_value=event.item_first_ignited_input,
        )

        if not (category_ok and room_ok and first_item_ok):
            continue

        item_combusted = normalise_raw_value(get_any(rule, ["item_combusted"]))

        if item_combusted is None:
            continue

        # Prefer the most specific rule.
        # Item_First_Ignited is usually the strongest evidence, so give it
        # slightly more weight than category/room.
        score = category_score + room_score + (2 * first_item_score)

        matches.append((score, item_combusted, rule))

    if not matches:
        return None

    max_score = max(score for score, _, _ in matches)
    best_items = sorted(
        {
            item
            for score, item, _ in matches
            if score == max_score
        }
    )

    if len(best_items) == 1:
        return best_items[0]

    raise BlockingResolutionError(
        "Ambiguous conditional item inference mapping for incident "
        f"{event.incident_id}. Top-ranked rules resolve to different "
        f"item_combusted values: {best_items}"
    )




# -----------------------------------------------------------------------------
# Area-band resolution
# -----------------------------------------------------------------------------

def resolve_total_damage_area_band(
    *,
    fire_damage_band: Optional[str],
    total_damage_band: Optional[str],
    mappings: MappingWorkbook,
    event: PreparedFireEvent,
    warnings: list[FireEventWarning],
) -> None:
    """
    Resolve the total-damage area band using the traffic-light rule.

    Rule
    ----
    n_tiers = total_damage_band_index - fire_damage_band_index

    n_tiers < 3:
        green, use as recorded

    n_tiers == 3:
        orange, use as recorded but flag as suspicious

    n_tiers > 3:
        red, treat as improbable and cap the model-facing total damage band at
        fire damage band + 3 tiers
    """
    fire_band = normalise_raw_value(fire_damage_band)
    total_band = normalise_raw_value(total_damage_band)

    event.building_fire_damage_area_input = fire_band
    event.building_total_damage_area_input = total_band

    if fire_band is None:
        mark_omit_with_warning(
            event,
            warnings,
            mappings=mappings,
            warning_type="missing_required_fris_field",
            reason="missing_required_fris_field: building_fire_damage_area",
            fire_parameter="building_fire_damage_area",
            raw_value=fire_damage_band,
            fallback_text=(
                "Required FRIS field {fire_parameter} is missing/NULL; "
                "incident omitted before detailed resolution."
            ),
        )
        return

    if total_band is None:
        mark_omit_with_warning(
            event,
            warnings,
            mappings=mappings,
            warning_type="missing_required_fris_field",
            reason="missing_required_fris_field: building_total_damage_area",
            fire_parameter="building_total_damage_area",
            raw_value=total_damage_band,
            fallback_text=(
                "Required FRIS field {fire_parameter} is missing/NULL; "
                "incident omitted before detailed resolution."
            ),
        )
        return

    fire_key = normalise_lookup_key(fire_band)
    total_key = normalise_lookup_key(total_band)

    if fire_key not in mappings.area_band_index:
        raise BlockingResolutionError(
            f"Area-band mapping incomplete. building_fire_damage_area value "
            f"'{fire_band}' is not in {TABLE_MAPPING_AREA_BANDS}."
        )

    if total_key not in mappings.area_band_index:
        raise BlockingResolutionError(
            f"Area-band mapping incomplete. building_total_damage_area value "
            f"'{total_band}' is not in {TABLE_MAPPING_AREA_BANDS}."
        )

    fire_idx = mappings.area_band_index[fire_key]
    total_idx = mappings.area_band_index[total_key]
    n_tiers = total_idx - fire_idx

    event.fire_damage_band_index = fire_idx
    event.total_damage_band_index = total_idx
    event.total_damage_tier_difference = n_tiers

    if n_tiers < 3:
        event.building_total_damage_area_for_model = total_band
        event.add_note("total_damage_area_traffic_light=green")
        return

    if n_tiers == 3:
        event.building_total_damage_area_for_model = total_band
        event.data_quality_status = "warning"
        event.suspicious_fields = append_delimited(
            event.suspicious_fields,
            "building_total_damage_area",
        )
        event.add_note("total_damage_area_traffic_light=orange")
        append_warning(
            warnings,
            mappings=mappings,
            incident_id=event.incident_id,
            source_id=event.source_id,
            warning_type="suspicious_total_damage_area_band",
            fire_parameter="building_total_damage_area",
            raw_value=total_band,
            resolved_value=total_band,
            template_values={
                "fire_damage_band": fire_band,
                "total_damage_band": total_band,
                "n_tiers": n_tiers,
            },
            fallback_text=(
                "Total damage area band is {n_tiers} tiers above the fire "
                "damage area band. The value has been retained but flagged."
            ),
        )
        return

    # Red route: cap at fire band + 3 tiers.  We need the reverse lookup from
    # index to display label.  If multiple labels somehow share the same index,
    # use the first one in the mapping table order.
    target_idx = fire_idx + 3
    capped_band = area_band_label_from_index(mappings.area_band_index, target_idx)
    event.building_total_damage_area_for_model = capped_band
    event.data_quality_status = "warning"
    event.suspicious_fields = append_delimited(
        event.suspicious_fields,
        "building_total_damage_area",
    )
    event.add_note("total_damage_area_traffic_light=red_capped")

    append_warning(
        warnings,
        mappings=mappings,
        incident_id=event.incident_id,
        source_id=event.source_id,
        warning_type="capped_total_damage_area_band",
        fire_parameter="building_total_damage_area",
        raw_value=total_band,
        resolved_value=capped_band,
        template_values={
            "fire_damage_band": fire_band,
            "total_damage_band": total_band,
            "model_total_damage_band": capped_band,
            "n_tiers": n_tiers,
        },
        fallback_text=(
            "Total damage area band is {n_tiers} tiers above the fire damage "
            "area band. This has been treated as improbable and the "
            "model-facing total damage band has been capped to "
            "{model_total_damage_band}."
        ),
    )


def area_band_label_from_index(area_band_index: dict[str, int], target_index: int) -> str:
    """
    Return a display label for one area-band index.
    """
    reverse: dict[int, str] = {}
    for key, idx in area_band_index.items():
        reverse.setdefault(idx, key)

    if target_index not in reverse:
        max_idx = max(reverse) if reverse else 0
        target_index = min(target_index, max_idx)

    # The stored key is lower-case.  Prefer the canonical default label if
    # available, otherwise return the key itself.
    key = reverse[target_index]
    for label in DEFAULT_AREA_BAND_ORDER:
        if normalise_lookup_key(label) == key:
            return label
    return key


# -----------------------------------------------------------------------------
# SQLite helpers
# -----------------------------------------------------------------------------

def list_tables(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table';"
    ).fetchall()
    return {str(r[0]) for r in rows}


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({quote_ident(table)});").fetchall()
    return [str(r[1]) for r in rows]


def first_existing_column(columns: Iterable[str], aliases: Iterable[str]) -> Optional[str]:
    by_lower = {c.lower(): c for c in columns}
    for alias in aliases:
        if alias.lower() in by_lower:
            return by_lower[alias.lower()]
    return None


def fetch_table_rows(conn: sqlite3.Connection, table: str) -> list[dict[str, Any]]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(f"SELECT * FROM {quote_ident(table)};")
    return [dict(row) for row in cur.fetchall()]


def distinct_column_values(conn: sqlite3.Connection, table: str, column: str) -> list[Any]:
    cur = conn.execute(
        f"SELECT DISTINCT {quote_ident(column)} FROM {quote_ident(table)};"
    )
    return [row[0] for row in cur.fetchall()]


def row_get(row: sqlite3.Row | dict[str, Any], canonical_name: str) -> Any:
    """
    Get a value from a staged row using the alias list for a canonical field.
    """
    if isinstance(row, sqlite3.Row):
        data = dict(row)
    else:
        data = row

    lower_to_name = {str(k).lower(): k for k in data.keys()}
    for alias in COL_ALIASES[canonical_name]:
        actual = lower_to_name.get(alias.lower())
        if actual is not None:
            return data.get(actual)
    return None


def quote_ident(name: str) -> str:
    """
    Quote a SQLite identifier.

    Table/column names in this script are controlled constants or PRAGMA output,
    but quoting avoids problems with unusual column names from staging tables.
    """
    return '"' + name.replace('"', '""') + '"'


def insert_dict_adaptive(
    conn: sqlite3.Connection,
    *,
    table: str,
    values: dict[str, Any],
) -> None:
    """
    Insert one row, keeping only fields that exist in the current table.

    This is useful while the schema is still being updated.  It also means the
    scripts can be reviewed before every new field has been added to init_db.
    """
    cols = table_columns(conn, table)
    write_values = {k: v for k, v in values.items() if k in cols}

    if not write_values:
        raise BlockingResolutionError(
            f"No matching columns to insert into {table}. Check schema."
        )

    col_sql = ", ".join(quote_ident(c) for c in write_values.keys())
    q_sql = ", ".join("?" for _ in write_values)

    conn.execute(
        f"INSERT INTO {quote_ident(table)} ({col_sql}) VALUES ({q_sql});",
        list(write_values.values()),
    )


def insert_fire_events_and_warnings(
    conn: sqlite3.Connection,
    *,
    events: list[PreparedFireEvent],
    warnings: list[FireEventWarning],
    overwrite: bool = False,
    input_type: str = INPUT_TYPE_FRIS,
    keep_omitted_events: bool = False,
) -> dict[str, int]:
    """
    Insert prepared events and warnings into the model-facing tables.

    Parameters
    ----------
    overwrite:
        If true, delete existing rows for this input_type where the schema allows
        it.  If an older table does not yet have input_type, delete all rows.

    keep_omitted_events:
        Development/debug option. If true, omitted incidents are also inserted into
        fire_events with omit_from_model = yes. This requires a schema that allows
        partially resolved omitted rows. In normal use this should be false: omitted
        incidents are skipped from fire_events, but their warnings are still inserted
        into fire_event_warnings.
    """
    existing = list_tables(conn)
    for table in [TABLE_FIRE_EVENTS, TABLE_FIRE_EVENT_WARNINGS]:
        if table not in existing:
            raise BlockingResolutionError(f"Required output table missing: {table}")

    if overwrite:
        delete_existing_fire_event_rows(conn, input_type=input_type)

    n_events = 0
    n_omitted_events = 0

    for event in events:
        if event.omit_from_model == "yes":
            n_omitted_events += 1
            if not keep_omitted_events:
                continue
        insert_dict_adaptive(conn, table=TABLE_FIRE_EVENTS, values=event.to_insert_dict())
        n_events += 1

    for warning in warnings:
        insert_dict_adaptive(
            conn,
            table=TABLE_FIRE_EVENT_WARNINGS,
            values=warning.to_insert_dict(),
        )

    return {
        "events_inserted": n_events,
        "warnings_inserted": len(warnings),
        "omitted_events_inserted": n_omitted_events if keep_omitted_events else 0,
    }


def delete_existing_fire_event_rows(
    conn: sqlite3.Connection,
    *,
    input_type: str,
) -> None:
    """
    Delete existing model-facing rows before rebuilding.

    Newer schema:
        delete only rows for the selected input_type.

    Older schema:
        delete all fire_events and fire_event_warnings because there is no safe
        input_type discriminator.
    """
    event_cols = table_columns(conn, TABLE_FIRE_EVENTS)
    warning_cols = table_columns(conn, TABLE_FIRE_EVENT_WARNINGS)

    if "input_type" in event_cols:
        # Delete warnings first to avoid FK issues if those are later added.
        if "input_type" in warning_cols:
            conn.execute(
                f"DELETE FROM {quote_ident(TABLE_FIRE_EVENT_WARNINGS)} WHERE input_type = ?;",
                (input_type,),
            )
        elif "incident_id" in warning_cols:
            conn.execute(
                f"""
                DELETE FROM {quote_ident(TABLE_FIRE_EVENT_WARNINGS)}
                WHERE incident_id IN (
                    SELECT incident_id
                    FROM {quote_ident(TABLE_FIRE_EVENTS)}
                    WHERE input_type = ?
                );
                """,
                (input_type,),
            )

        conn.execute(
            f"DELETE FROM {quote_ident(TABLE_FIRE_EVENTS)} WHERE input_type = ?;",
            (input_type,),
        )
        return

    # Fallback for the older single-event-only schema.
    conn.execute(f"DELETE FROM {quote_ident(TABLE_FIRE_EVENT_WARNINGS)};")
    conn.execute(f"DELETE FROM {quote_ident(TABLE_FIRE_EVENTS)};")


# -----------------------------------------------------------------------------
# Internal mapping table loaders
# -----------------------------------------------------------------------------

def _load_warning_templates(conn: sqlite3.Connection) -> dict[str, WarningTemplate]:
    rows = fetch_table_rows(conn, TABLE_MAPPING_WARNINGS)
    templates: dict[str, WarningTemplate] = {}

    for row in rows:
        warning_type = clean_code(get_any(row, ["warning_type"]))
        if not warning_type:
            continue

        if warning_type in templates:
            raise BlockingResolutionError(
                f"Duplicate warning_type in {TABLE_MAPPING_WARNINGS}: {warning_type}"
            )

        severity = clean_code(get_any(row, ["warning_severity"])) or "warning"
        if severity not in VALID_WARNING_SEVERITIES:
            raise BlockingResolutionError(
                f"Invalid warning_severity '{severity}' for warning_type '{warning_type}'."
            )

        text = normalise_raw_value(get_any(row, ["warning_text", "message", "text"]))
        if not text:
            raise BlockingResolutionError(
                f"warning_type '{warning_type}' is missing warning_text."
            )

        templates[warning_type] = WarningTemplate(
            warning_type=warning_type,
            warning_text=text,
            warning_severity=severity,
            warning_category=clean_code(get_any(row, ["warning_category"])),
            notes=to_optional_str(get_any(row, ["notes"])),
        )

    return templates


def _load_keyed_mapping(
    conn: sqlite3.Connection,
    *,
    table: str,
    key_aliases: list[str],
    display_name: str,
) -> dict[str, dict[str, Any]]:
    rows = fetch_table_rows(conn, table)
    out: dict[str, dict[str, Any]] = {}

    for row in rows:
        raw_key = get_any(row, key_aliases)
        key = normalise_lookup_key(raw_key)
        if not key:
            continue

        if key in out:
            raise BlockingResolutionError(
                f"Duplicate key in {display_name} ({table}): {raw_key!r}"
            )

        out[key] = row

    if not out:
        raise BlockingResolutionError(f"No usable rows found in {table}.")

    return out


def _load_area_band_index(conn: sqlite3.Connection) -> dict[str, int]:
    rows = fetch_table_rows(conn, TABLE_MAPPING_AREA_BANDS)
    out: dict[str, int] = {}

    for row in rows:
        label = get_any(row, ["area_band", "band", "input_value"])
        key = normalise_lookup_key(label)
        if not key:
            continue

        raw_index = get_any(row, ["band_order", "area_band_index", "band_index"])
        if raw_index is None:
            raise BlockingResolutionError(
                f"Area band '{label}' is missing band_order in {TABLE_MAPPING_AREA_BANDS}."
            )

        try:
            idx = int(raw_index)
        except Exception as exc:
            raise BlockingResolutionError(
                f"Area band '{label}' has non-integer band_order: {raw_index!r}"
            ) from exc

        if key in out:
            raise BlockingResolutionError(
                f"Duplicate area band in {TABLE_MAPPING_AREA_BANDS}: {label!r}"
            )

        out[key] = idx

    if not out:
        raise BlockingResolutionError(f"No usable area bands found in {TABLE_MAPPING_AREA_BANDS}.")

    return out


def _validate_warning_types_used(
    *,
    mapping_rows: list[dict[str, Any]],
    warning_templates: dict[str, WarningTemplate],
) -> None:
    """
    Check that configured warning_type values exist in the warning catalogue.

    The mapping workbook can contain more than one warning_type in a single cell,
    separated by semi-colons. This is easier to maintain in Excel than adding
    duplicate mapping rows for every warning. The resolver therefore splits
    these cells before checking each warning_type against fire_event_mapping_warnings.
    """
    missing: set[str] = set()

    for row in mapping_rows:
        for warning_type in split_warning_types(get_any(row, ["warning_type"])):
            if warning_type not in warning_templates:
                missing.add(warning_type)

    if missing:
        raise BlockingResolutionError(
            "Mapping table(s) reference warning_type value(s) missing from "
            f"{TABLE_MAPPING_WARNINGS}: " + ", ".join(sorted(missing))
        )



def get_any(row: dict[str, Any], aliases: Iterable[str]) -> Any:
    lower_to_name = {str(k).lower(): k for k in row.keys()}
    for alias in aliases:
        actual = lower_to_name.get(alias.lower())
        if actual is not None:
            return row.get(actual)
    return None
