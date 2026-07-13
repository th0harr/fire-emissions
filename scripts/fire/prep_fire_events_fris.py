# scripts/fire/prep_fire_events_fris.py
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Optional

from scripts.fire.fire_event_resolver import (
    append_warning,
    AREA_BAND_NONE,
    BlockingResolutionError,
    BuildSummary,
    COL_ALIASES,
    FIRE_CATEGORIES_REQUIRING_ROOM,
    INPUT_TYPE_FRIS,
    TABLE_FRIS_STAGING,
    FireEventWarning,
    MappingWorkbook,
    PreparedFireEvent,
    append_warning,
    clean_code,
    db_connect,
    get_any,
    load_fire_event_mappings,
    mark_omit_with_warning,
    normalise_heat_smoke_damage_only,
    normalise_lookup_key,
    normalise_raw_value,
    parse_bool_like,
    parse_fiscal_year,
    resolve_conditionally_inferred_item,
    resolve_total_damage_area_band,
    row_get,
    split_ignition_source_all,
    table_columns,
    validate_mapping_coverage_for_fris,
    VALID_SINGLE_ITEM_STATUSES,
)


# -----------------------------------------------------------------------------
# Public FRIS preparation function
# -----------------------------------------------------------------------------

def prepare_fris_events(
    *,
    conn: sqlite3.Connection,
    run_mapping_coverage_check: bool = True,
) -> tuple[list[PreparedFireEvent], list[FireEventWarning], BuildSummary]:
    """
    Build model-facing fire event rows from the FRIS staging table.

    This function does not write to fire_events.  It only prepares the event rows
    and warnings.  The caller decides whether this is a dry run or an applied DB
    write.

    Main design principle
    ---------------------
    - Missing/NULL values in one FRIS incident omit that incident.
    - Present raw categories missing from mapping tables block the whole build.

    That distinction is important.  An unusable incident row is a data-quality
    issue.  An unmapped category is a configuration issue and could otherwise
    cause us to miss a new FRIS option silently.
    """
    mappings = load_fire_event_mappings(conn)

    if run_mapping_coverage_check:
        validate_mapping_coverage_for_fris(conn, mappings)

    rows = _read_fris_staging_rows(conn)

    events: list[PreparedFireEvent] = []
    warnings: list[FireEventWarning] = []
    summary = BuildSummary(rows_read=len(rows), blocking_checks_passed=True)

    for row in rows:
        event, row_warnings = prepare_one_fris_event(row=row, mappings=mappings)
        events.append(event)
        warnings.extend(row_warnings)

    summary.rows_prepared = len(events)
    summary.rows_omitted = sum(1 for e in events if e.omit_from_model == "yes")
    summary.rows_insertable = len(events) - summary.rows_omitted
    summary.warnings = len(warnings)

    return events, warnings, summary


def prepare_fris_events_from_db(
    db_path: str | Path,
    *,
    run_mapping_coverage_check: bool = True,
) -> tuple[list[PreparedFireEvent], list[FireEventWarning], BuildSummary]:
    """
    Convenience wrapper for tests and notebooks.
    """
    conn = db_connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return prepare_fris_events(
            conn=conn,
            run_mapping_coverage_check=run_mapping_coverage_check,
        )
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Row-level resolver
# -----------------------------------------------------------------------------

def prepare_one_fris_event(
    *,
    row: sqlite3.Row | dict[str, Any],
    mappings: MappingWorkbook,
) -> tuple[PreparedFireEvent, list[FireEventWarning]]:
    """
    Resolve one FRIS incident row.

    The routes are kept as small functions so that future users can follow the
    decisions in the same order as the flow chart:
        1. first-pass raw NULL check
        2. dwelling route
        3. area-band route
        4. fire-spread route
        5. room route
        6. ignition/item route
        7. final data-quality summary

    Important:
        A FRIS text value of "None" is valid and is not treated as missing.
        A FRIS text value of "NULL", blank or NaN-like value makes the row
        unusable. If a row contains several NULL values, only the first one is
        counted so that omission summaries are not artificially inflated.
    """
    warnings: list[FireEventWarning] = []

    incident_id = normalise_raw_value(row_get(row, "incident_id"))
    source_id = normalise_raw_value(row_get(row, "source_id"))

    # Incident_Id should exist in the FRIS data.  If it is missing, create a
    # clearly synthetic identifier so the warning can still be tracked.
    if incident_id is None:
        incident_id = f"fris_missing_incident_id_{id(row)}"

    event = PreparedFireEvent(
        incident_id=incident_id,
        source_id=source_id,
        input_type=INPUT_TYPE_FRIS,
    )

    _copy_raw_metadata(event, row)

    # -------------------------------------------------
    # FIRST-PASS RAW FRIS VALIDATION
    # -------------------------------------------------
    # If any FRIS content field contains NULL / blank / NaN, treat the whole
    # incident row as unusable.
    #
    # This is deliberately done once, at the start of row resolution. Some
    # invalid FRIS rows contain NULL in many columns. If we allowed the row to
    # continue through every resolver route, the same incident would be counted
    # several times in the omission summaries.
    #
    # The text value "None" is still valid. For example:
    #
    #     Building_Fire_Damage_Area = None
    #
    # means no recorded fire-damage area. It does not mean the field is missing.
    if _omit_if_required_raw_field_missing(event, row, mappings, warnings):
        _finalise_event_quality(event, warnings)
        return event, warnings


    # -------------------------------------------------
    # HEAT / SMOKE FLAG
    # -------------------------------------------------
    # This is now safe to parse because the first-pass check above has already
    # removed rows where the raw value was NULL / blank / unusable.
    heat_smoke_value = normalise_heat_smoke_damage_only(
        row_get(row, "heat_smoke_damage_only")
    )
    event.heat_smoke_damage_only = heat_smoke_value

    if heat_smoke_value == "NULL":
        mark_omit_with_warning(
            event,
            warnings,
            mappings=mappings,
            warning_type="missing_required_fris_field",
            reason="missing_required_fris_field: heat_smoke_damage_only",
            fire_parameter="heat_smoke_damage_only",
            raw_value=row_get(row, "heat_smoke_damage_only"),
            fallback_text=(
                "Required FRIS field {fire_parameter} is missing, NULL, or "
                "unexpected; incident omitted before detailed resolution."
            ),
        )
        _finalise_event_quality(event, warnings)
        return event, warnings

    # -------------------------------------------------
    # ROUTE 1: DWELLING
    # -------------------------------------------------
    _resolve_dwelling_route(event, row, mappings, warnings)
    if event.omit_from_model == "yes":
        _finalise_event_quality(event, warnings)
        return event, warnings

    # -------------------------------------------------
    # ROUTE 2: AREA BANDS
    # -------------------------------------------------
    _resolve_area_band_route(event, row, mappings, warnings)
    if event.omit_from_model == "yes":
        _finalise_event_quality(event, warnings)
        return event, warnings

    # -------------------------------------------------
    # ROUTE 3: FIRE SPREAD
    # -------------------------------------------------
    _resolve_fire_spread_route(event, row, mappings, warnings)
    if event.omit_from_model == "yes":
        _finalise_event_quality(event, warnings)
        return event, warnings

    # -------------------------------------------------
    # ROUTE 4: ROOM
    # -------------------------------------------------
    _resolve_room_route(event, row, mappings, warnings)
    if event.omit_from_model == "yes":
        _finalise_event_quality(event, warnings)
        return event, warnings

    # -------------------------------------------------
    # ROUTE 5: IGNITION / ITEM
    # -------------------------------------------------
    _resolve_ignition_item_route(event, row, mappings, warnings)

    _finalise_event_quality(event, warnings)
    return event, warnings


# -----------------------------------------------------------------------------
# Route 0: raw metadata / required fields
# -----------------------------------------------------------------------------

def _copy_raw_metadata(event: PreparedFireEvent, row: sqlite3.Row | dict[str, Any]) -> None:
    """
    Copy raw fields into the prepared event object.

    These are retained even where later routes replace them with model-facing
    categories.  This makes it easier to audit how FRIS records have been
    resolved.
    """
    fiscal_year_start, fiscal_year_end = parse_fiscal_year(row_get(row, "fiscal_year"))
    event.fiscal_year_start = fiscal_year_start
    event.fiscal_year_end = fiscal_year_end

    event.property_type_input = normalise_raw_value(row_get(row, "property_type_3"))
    event.dwelling_type_input = event.property_type_input
    event.fire_spread_category_input = normalise_raw_value(row_get(row, "extent_of_damage"))
    event.room_of_origin_input = normalise_raw_value(row_get(row, "fire_start_location"))
    event.heat_smoke_damage_only_input = normalise_raw_value(row_get(row, "heat_smoke_damage_only"))

    event.building_fire_damage_area_input = normalise_raw_value(row_get(row, "building_fire_damage_area"))
    event.building_total_damage_area_input = normalise_raw_value(row_get(row, "building_total_damage_area"))
    event.room_of_origin_size_input = normalise_raw_value(row_get(row, "building_room_origin_size"))
    event.origin_floor_size_input = normalise_raw_value(row_get(row, "building_floor_origin_size"))

    event.ignition_source_all_input = normalise_raw_value(row_get(row, "ignition_source_all"))
    event.item_first_ignited_input = normalise_raw_value(row_get(row, "item_first_ignited"))
    event.item_causing_spread_input = normalise_raw_value(row_get(row, "item_causing_spread"))
    event.fire_size_on_arrival_input = normalise_raw_value(row_get(row, "fire_size_on_arrival"))
    event.rapid_fire_growth_input = normalise_raw_value(row_get(row, "rapid_fire_growth"))
    event.distance_to_adjoining_property_input = normalise_raw_value(
        row_get(row, "distance_to_adjoining_property")
    )

    category, source = split_ignition_source_all(event.ignition_source_all_input)
    event.ignition_source_category = category
    event.ignition_source = source


# Omit NULL rows

def _is_unusable_fris_null(value: Any) -> bool:
    """
    Return True only for genuinely unusable FRIS missing values.

    Important distinction:
        "NULL" = unusable missing FRIS input
        "None" = valid FRIS response/category

    Therefore, text "None" must be retained for later resolver logic.
    """
    if value is None:
        return True

    # Handles float("nan") without requiring pandas.
    try:
        if value != value:
            return True
    except Exception:
        pass

    text = str(value).strip()

    if text == "":
        return True

    return text.lower() in {
        "null",
        "nan",
        "na",
        "n/a",
    }


def _omit_if_required_raw_field_missing(
    event: PreparedFireEvent,
    row: sqlite3.Row | dict[str, Any],
    mappings: MappingWorkbook,
    warnings: list[FireEventWarning],
) -> bool:
    """
    Apply the first-pass FRIS NULL checks.

    Only the first unusable field is reported. This is deliberate, because
    invalid FRIS rows often contain NULL in many downstream fields. Reporting
    only the first NULL field prevents omission counts from being artificially
    inflated by counting the same row multiple times.

    Important:
        "NULL", blank, SQL NULL, NaN -> omit row
        "None"                       -> valid FRIS response, do not omit
    """
    required_order = [
        ("fiscal_year", "fiscal_year"),
        ("property_type_3", "property_type_3"),
        ("heat_smoke_damage_only", "heat_smoke_damage_only"),
        ("ignition_source_all", "ignition_source_all"),
        ("fire_size_on_arrival", "fire_size_on_arrival"),
        ("fire_start_location", "fire_start_location"),
        ("item_first_ignited", "item_first_ignited"),
        ("item_causing_spread", "item_causing_spread"),
        ("extent_of_damage", "extent_of_damage"),
        ("rapid_fire_growth", "rapid_fire_growth"),
        ("building_room_origin_size", "building_room_origin_size"),
        ("building_floor_origin_size", "building_floor_origin_size"),
        ("building_fire_damage_area", "building_fire_damage_area"),
        ("building_total_damage_area", "building_total_damage_area"),
        ("distance_to_adjoining_property", "distance_to_adjoining_property"),
    ]

    for canonical_name, fire_parameter in required_order:
        raw_value = row_get(row, canonical_name)

        if _is_unusable_fris_null(raw_value):
            mark_omit_with_warning(
                event,
                warnings,
                mappings=mappings,
                warning_type="missing_required_fris_field",
                reason=f"missing_required_fris_field: {fire_parameter}",
                fire_parameter=fire_parameter,
                raw_value=raw_value,
                fallback_text=(
                    "Required FRIS field {fire_parameter} is NULL / blank / "
                    "unusable; incident omitted before detailed resolution."
                ),
            )

            event.add_note("omitted_at_first_unusable_raw_fris_field")
            return True

    return False


# -----------------------------------------------------------------------------
# Route 1: dwelling
# -----------------------------------------------------------------------------

def _resolve_dwelling_route(
    event: PreparedFireEvent,
    row: sqlite3.Row | dict[str, Any],
    mappings: MappingWorkbook,
    warnings: list[FireEventWarning],
) -> None:
    """
    Resolve FRIS Property_Type_3 to the model dwelling category.
    """
    raw_property = normalise_raw_value(row_get(row, "property_type_3"))
    key = normalise_lookup_key(raw_property)

    mapping = mappings.dwellings_by_property.get(key)
    if mapping is None:
        raise BlockingResolutionError(
            f"Dwelling mapping incomplete. Property_Type_3 value '{raw_property}' "
            f"is not in the dwellings mapping table."
        )

    event.dwelling_type = clean_code(get_any(mapping, ["dwelling_type"]))
    event.dwelling_type_proxy = clean_code(get_any(mapping, ["dwelling_type_proxy"]))
    event.dwelling_type_for_model = event.dwelling_type_proxy or event.dwelling_type
    event.occupancy = clean_code(get_any(mapping, ["occupancy_override", "occupancy"]))

    if parse_bool_like(get_any(mapping, ["omit_from_model"]), default=False):
        warning_type = clean_code(get_any(mapping, ["warning_type"])) or "unsupported_dwelling_type"
        mark_omit_with_warning(
            event,
            warnings,
            mappings=mappings,
            warning_type=warning_type,
            reason=f"unsupported_dwelling_type: {raw_property}",
            fire_parameter="property_type_3",
            raw_value=raw_property,
            template_values={
                "dwelling_type": event.dwelling_type or raw_property,
                "dwelling_type_proxy": event.dwelling_type_proxy,
            },
            fallback_text=(
                "This FRIS dwelling type is outside the current domestic "
                "inventory basis and has been omitted from the model-facing dataset."
            ),
        )
        return

    if event.dwelling_type_proxy:
        append_warning(
            warnings,
            mappings=mappings,
            incident_id=event.incident_id,
            source_id=event.source_id,
            warning_type="substitute_proxy_dwelling_type",
            fire_parameter="property_type_3",
            raw_value=raw_property,
            resolved_value=event.dwelling_type_proxy,
            template_values={
                "dwelling_type": event.dwelling_type or raw_property,
                "dwelling_type_proxy": event.dwelling_type_proxy,
            },
            fallback_text=(
                "FRIS records this incident as {dwelling_type}, but specific "
                "dwelling-size and inventory assumptions are not currently "
                "available for this category. The incident has therefore been "
                "modelled using {dwelling_type_proxy} as a dwelling proxy."
            ),
        )

    configured_warning = clean_code(get_any(mapping, ["warning_type"]))
    if configured_warning and configured_warning != "substitute_proxy_dwelling_type":
        append_warning(
            warnings,
            mappings=mappings,
            incident_id=event.incident_id,
            source_id=event.source_id,
            warning_type=configured_warning,
            fire_parameter="property_type_3",
            raw_value=raw_property,
            resolved_value=event.dwelling_type_for_model,
            template_values={
                "dwelling_type": event.dwelling_type or raw_property,
                "dwelling_type_proxy": event.dwelling_type_proxy,
                "occupancy": event.occupancy,
            },
        )


# -----------------------------------------------------------------------------
# Route 2: area bands
# -----------------------------------------------------------------------------

def _resolve_area_band_route(
    event: PreparedFireEvent,
    row: sqlite3.Row | dict[str, Any],
    mappings: MappingWorkbook,
    warnings: list[FireEventWarning],
) -> None:
    """
    Resolve fire damage and total damage area bands.
    """
    fire_damage_band = normalise_raw_value(row_get(row, "building_fire_damage_area"))
    total_damage_band = normalise_raw_value(row_get(row, "building_total_damage_area"))

    if event.heat_smoke_damage_only == "yes" and fire_damage_band != AREA_BAND_NONE:
        # Heat/smoke-only cases should not really have fire damage area.  We keep
        # the original field, but flag the inconsistency and let the heat/smoke
        # route control the fire_spread_category.
        append_warning(
            warnings,
            mappings=mappings,
            incident_id=event.incident_id,
            source_id=event.source_id,
            warning_type="heat_smoke_fire_area_inconsistency",
            fire_parameter="building_fire_damage_area",
            raw_value=fire_damage_band,
            resolved_value=AREA_BAND_NONE,
            template_values={"fire_damage_band": fire_damage_band},
            fallback_text=(
                "heat_smoke_damage_only is yes, but building_fire_damage_area "
                "is {fire_damage_band}. The incident has been treated as "
                "heat/smoke-only for fire-spread resolution."
            ),
        )

    resolve_total_damage_area_band(
        fire_damage_band=fire_damage_band,
        total_damage_band=total_damage_band,
        mappings=mappings,
        event=event,
        warnings=warnings,
    )


# -----------------------------------------------------------------------------
# Route 3: fire spread
# -----------------------------------------------------------------------------

def _resolve_fire_spread_route(
    event: PreparedFireEvent,
    row: sqlite3.Row | dict[str, Any],
    mappings: MappingWorkbook,
    warnings: list[FireEventWarning],
) -> None:
    """
    Resolve final fire_spread_category.
    """
    if event.heat_smoke_damage_only == "yes":
        event.fire_spread_category = "heat_smoke_damage_only"
        event.add_note("fire_spread_from_heat_smoke_damage_only")
        return

    extent = normalise_raw_value(row_get(row, "extent_of_damage"))
    key = normalise_lookup_key(extent)
    mapping = mappings.fire_cat_by_extent.get(key)

    if mapping is None:
        raise BlockingResolutionError(
            f"Fire category mapping incomplete. Extent_of_Damage value "
            f"'{extent}' is not in the fire_cat mapping table."
        )

    preliminary = clean_code(get_any(mapping, ["fire_spread_category", "resolved_fire_spread_category"]))
    if not preliminary:
        raise BlockingResolutionError(
            f"Fire category mapping for '{extent}' is missing fire_spread_category."
        )

    if parse_bool_like(get_any(mapping, ["omit_from_model"]), default=False):
        warning_type = clean_code(get_any(mapping, ["warning_type"])) or "unsupported_fire_spread_category"
        mark_omit_with_warning(
            event,
            warnings,
            mappings=mappings,
            warning_type=warning_type,
            reason=f"unsupported_fire_spread_category: {extent}",
            fire_parameter="extent_of_damage",
            raw_value=extent,
            resolved_value=preliminary,
            fallback_text=(
                "This FRIS fire-spread category is not currently supported and "
                "has been omitted from the model-facing dataset."
            ),
        )
        event.fire_spread_category = preliminary
        return

    requires_area_rule = parse_bool_like(
        get_any(
            mapping,
            [
                "occupancy_dependent",
                "requires_occupancy_area_rule",
                "requires_occupancy",
                "requires_fire_area_band",
            ],
        ),
        default=False,
    )

    if requires_area_rule:
        event.fire_spread_category = _resolve_occupancy_area_fire_spread(
            preliminary_category=preliminary,
            occupancy=event.occupancy,
            fire_damage_band_index=event.fire_damage_band_index,
            extent_of_damage=extent,
            event=event,
            mappings=mappings,
            warnings=warnings,
        )
    else:
        event.fire_spread_category = preliminary

    configured_warning = clean_code(get_any(mapping, ["warning_type"]))
    if configured_warning:
        append_warning(
            warnings,
            mappings=mappings,
            incident_id=event.incident_id,
            source_id=event.source_id,
            warning_type=configured_warning,
            fire_parameter="extent_of_damage",
            raw_value=extent,
            resolved_value=event.fire_spread_category,
            template_values={
                "extent_of_damage": extent,
                "fire_spread_category": event.fire_spread_category,
            },
        )


def _resolve_occupancy_area_fire_spread(
    *,
    preliminary_category: str,
    occupancy: Optional[str],
    fire_damage_band_index: Optional[int],
    extent_of_damage: Optional[str],
    event: PreparedFireEvent,
    mappings: MappingWorkbook,
    warnings: list[FireEventWarning],
) -> str:
    """
    Apply the multi-occupancy extent/area rule.

    Current simplified rule
    -----------------------
    Single occupancy:
        floor/two-floor/more-than-two-floor spread is treated as multiple_rooms,
        and Whole building as entire_dwelling.

    Multiple occupancy:
        if fire damage is up to 50 m2, treat as multiple_rooms;
        if fire damage is above 50 m2, treat as entire_dwelling.

    The later model may estimate affected dwelling counts, but those values do
    not belong in fire_events at this stage.
    """
    text = (extent_of_damage or "").lower()

    if "whole building" in text:
        return "entire_dwelling"

    if occupancy == "single":
        return "multiple_rooms"

    if occupancy == "multiple":
        # Area band index 4 corresponds to 21-50 in the agreed area-band order.
        # Therefore 0..4 is not above 50 m2, and 5+ is above 50 m2.
        if fire_damage_band_index is not None and fire_damage_band_index <= 4:
            return "multiple_rooms"
        return "entire_dwelling"

    append_warning(
        warnings,
        mappings=mappings,
        incident_id=event.incident_id,
        source_id=event.source_id,
        warning_type="unknown_occupancy_fire_spread_rule",
        fire_parameter="property_type_3",
        raw_value=occupancy,
        resolved_value=preliminary_category,
        fallback_text=(
            "The fire-spread category requires an occupancy/area rule, but "
            "occupancy is unknown. The preliminary mapped category has been retained."
        ),
    )
    return preliminary_category


# -----------------------------------------------------------------------------
# Route 4: room
# -----------------------------------------------------------------------------

def _resolve_room_route(
    event: PreparedFireEvent,
    row: sqlite3.Row | dict[str, Any],
    mappings: MappingWorkbook,
    warnings: list[FireEventWarning],
) -> None:
    """
    Resolve Fire_Start_Location to the model room_of_origin where required.
    """
    if event.fire_spread_category not in FIRE_CATEGORIES_REQUIRING_ROOM:
        # The raw room is retained as metadata.  We do not omit entire-dwelling
        # or heat/smoke records purely because the room is unsupported.
        return

    raw_location = normalise_raw_value(row_get(row, "fire_start_location"))
    if raw_location is None:
        mark_omit_with_warning(
            event,
            warnings,
            mappings=mappings,
            warning_type="missing_required_fris_field",
            reason="missing_required_fris_field: fire_start_location",
            fire_parameter="fire_start_location",
            raw_value=row_get(row, "fire_start_location"),
            fallback_text=(
                "Required FRIS field {fire_parameter} is missing/NULL; "
                "incident omitted before room resolution."
            ),
        )
        return

    mapping = mappings.rooms_by_location.get(normalise_lookup_key(raw_location))
    if mapping is None:
        raise BlockingResolutionError(
            f"Room mapping incomplete. Fire_Start_Location value '{raw_location}' "
            f"is not in the rooms mapping table."
        )

    event.room_of_origin = clean_code(get_any(mapping, ["room_type", "room_of_origin"]))
    event.room_of_origin_proxy = clean_code(get_any(mapping, ["room_type_proxy", "room_proxy"]))

    if event.room_of_origin_proxy:
        event.room_of_origin = event.room_of_origin_proxy

    if parse_bool_like(get_any(mapping, ["omit_from_model"]), default=False):
        warning_type = clean_code(get_any(mapping, ["warning_type"])) or "unsupported_room_type"
        mark_omit_with_warning(
            event,
            warnings,
            mappings=mappings,
            warning_type=warning_type,
            reason=f"unsupported_room_type: {raw_location}",
            fire_parameter="fire_start_location",
            raw_value=raw_location,
            resolved_value=event.room_of_origin,
            fallback_text=(
                "This FRIS room/start-location category is not currently "
                "supported and has been omitted from the model-facing dataset."
            ),
        )
        return

    configured_warning = clean_code(get_any(mapping, ["warning_type"]))
    if configured_warning:
        append_warning(
            warnings,
            mappings=mappings,
            incident_id=event.incident_id,
            source_id=event.source_id,
            warning_type=configured_warning,
            fire_parameter="fire_start_location",
            raw_value=raw_location,
            resolved_value=event.room_of_origin,
            template_values={
                "fire_start_location": raw_location,
                "room_of_origin": event.room_of_origin,
            },
        )


# -----------------------------------------------------------------------------
# Route 5: ignition / item
# -----------------------------------------------------------------------------

def _resolve_ignition_item_route(
    event: PreparedFireEvent,
    row: sqlite3.Row | dict[str, Any],
    mappings: MappingWorkbook,
    warnings: list[FireEventWarning],
) -> None:
    """
    Resolve item_combusted for final single_item fires.

    If the fire is not final single_item, the item fields are retained for
    transparency only. They should not cause row omission.
    """
    if event.fire_spread_category != "single_item":
        event.add_note("item_fields_retained_for_transparency_only")
        return

    mapping = _find_item_mapping(event, mappings)

    if mapping is None:
        mark_omit_with_warning(
            event,
            warnings,
            mappings=mappings,
            warning_type="unmapped_single_item",
            reason="unmapped_single_item",
            fire_parameter="ignition_source_all",
            raw_value=event.ignition_source_all_input,
            template_values={
                "ignition_source_all": event.ignition_source_all_input,
                "item_first_ignited": event.item_first_ignited_input,
                "room_of_origin": event.room_of_origin,
            },
            fallback_text=(
                "This single-item incident could not be resolved to a usable "
                "inventory item or proxy and has been omitted."
            ),
        )

        # Use an allowed controlled value rather than "unmapped", because
        # "unmapped" is not part of the current fire_events single_item_status
        # CHECK constraint.
        event.single_item_status = "invalid_single_item"
        return

    event.single_item_status = clean_code(get_any(mapping, ["single_item_status"]))
    event.item_combusted = clean_code(
        get_any(mapping, ["item_combusted", "inventory_item", "item_proxy"])
    )

    # -------------------------------------------------
    # CHECK CONTROLLED STATUS
    # -------------------------------------------------
    # This catches mapping-workbook errors, not normal FRIS data problems.
    # conditionally_inferred_item is valid and must not be omitted here.
    if event.single_item_status not in VALID_SINGLE_ITEM_STATUSES:
        mark_omit_with_warning(
            event,
            warnings,
            mappings=mappings,
            warning_type=clean_code(get_any(mapping, ["warning_type"])) or "invalid_single_item",
            reason=f"invalid_single_item_status: {event.single_item_status}",
            fire_parameter="ignition_source_all",
            raw_value=event.ignition_source_all_input,
            resolved_value=event.item_combusted,
            fallback_text=(
                "This single-item incident maps to a single_item_status that is "
                "not a valid controlled value and has been omitted."
            ),
        )
        return

    # -------------------------------------------------
    # GENUINELY INVALID SINGLE-ITEM CASES
    # -------------------------------------------------
    if event.single_item_status == "invalid_single_item":
        mark_omit_with_warning(
            event,
            warnings,
            mappings=mappings,
            warning_type=clean_code(get_any(mapping, ["warning_type"])) or "invalid_single_item",
            reason="invalid_single_item_status: invalid_single_item",
            fire_parameter="ignition_source_all",
            raw_value=event.ignition_source_all_input,
            resolved_value=event.item_combusted,
            template_values={
                "ignition_source_all": event.ignition_source_all_input,
                "item_first_ignited": event.item_first_ignited_input,
                "room_of_origin": event.room_of_origin,
                "item_combusted": event.item_combusted,
            },
            fallback_text=(
                "This single-item incident has an ignition source/item context "
                "that cannot currently be assigned to a modelled inventory item "
                "and has been omitted."
            ),
        )
        return

    # -------------------------------------------------
    # CONDITIONAL ITEM INFERENCE
    # -------------------------------------------------
    # Some ignition sources are too broad to map directly to an inventory item.
    # For these, fire_event_mapping_items deliberately sets:
    #
    #     single_item_status = conditionally_inferred_item
    #
    # and the actual item_combusted is resolved from
    # fire_event_mapping_item_inference using contextual fields such as
    # Item_First_Ignited and room_of_origin.
    if event.single_item_status == "conditionally_inferred_item":
        inferred_item = resolve_conditionally_inferred_item(
            event=event,
            mappings=mappings,
        )

        if inferred_item is None:
            # This ignition/item route is only modelled if it matches one of the
            # explicitly configured conditional-inference scenarios.
            #
            # If no scenario matches, it is not a separate omission class. It is
            # simply an invalid single-item case for the current model.
            event.single_item_status = "invalid_single_item"

            mark_omit_with_warning(
                event,
                warnings,
                mappings=mappings,
                warning_type="invalid_single_item",
                reason="invalid_single_item_status: invalid_single_item",
                fire_parameter="item_first_ignited",
                raw_value=event.item_first_ignited_input,
                resolved_value=None,
                template_values={
                    "ignition_source_all": event.ignition_source_all_input,
                    "item_first_ignited": event.item_first_ignited_input,
                    "room_of_origin": event.room_of_origin,
                },
                fallback_text=(
                    "This single-item fire was marked as conditionally inferable, "
                    "but it did not match any configured item-inference scenario. "
                    "It has therefore been treated as an invalid single-item case "
                    "and omitted."
                ),
            )
            return


        event.item_combusted = inferred_item
        event.add_note("item_combusted_conditionally_inferred")

        append_warning(
            warnings,
            mappings=mappings,
            incident_id=event.incident_id,
            source_id=event.source_id,
            input_type=event.input_type,
            warning_type="conditionally_inferred_item_used",
            fire_parameter="item_first_ignited",
            raw_value=event.item_first_ignited_input,
            resolved_value=inferred_item,
            template_values={
                "ignition_source_all": event.ignition_source_all_input,
                "item_first_ignited": event.item_first_ignited_input,
                "room_of_origin": event.room_of_origin,
                "item_combusted": event.item_combusted,
            },
            fallback_text=(
                "The combusted item was inferred from Item_First_Ignited and "
                "incident context rather than mapped directly from "
                "Ignition_Source_All."
            ),
            fallback_severity="warning",
            fallback_category="model_assumption",
        )
        return

    # -------------------------------------------------
    # DIRECT / PROXY ITEM CASES
    # -------------------------------------------------
    if event.single_item_status in {"direct_inventory_item", "proxy_inventory_item"}:
        if normalise_raw_value(event.item_combusted) is None:
            mark_omit_with_warning(
                event,
                warnings,
                mappings=mappings,
                warning_type="missing_item_combusted",
                reason="missing_item_combusted",
                fire_parameter="ignition_source_all",
                raw_value=event.ignition_source_all_input,
                template_values={
                    "ignition_source_all": event.ignition_source_all_input,
                    "item_first_ignited": event.item_first_ignited_input,
                    "room_of_origin": event.room_of_origin,
                },
                fallback_text=(
                    "This single-item incident has a modelled item status, "
                    "but no item_combusted value was provided."
                ),
            )
            return

        configured_warning = clean_code(get_any(mapping, ["warning_type"]))

        if configured_warning:
            append_warning(
                warnings,
                mappings=mappings,
                incident_id=event.incident_id,
                source_id=event.source_id,
                input_type=event.input_type,
                warning_type=configured_warning,
                fire_parameter="ignition_source_all",
                raw_value=event.ignition_source_all_input,
                resolved_value=event.item_combusted,
                template_values={
                    "ignition_source_all": event.ignition_source_all_input,
                    "item_first_ignited": event.item_first_ignited_input,
                    "room_of_origin": event.room_of_origin,
                    "item_combusted": event.item_combusted,
                },
            )

        return

def _find_item_mapping(event: PreparedFireEvent, mappings: MappingWorkbook) -> Optional[dict[str, Any]]:
    """
    Find the best item mapping for a single-item fire.

    Rows can be broad or contextual.  The matching score prefers rows that match
    more fields.  Blank fields in the mapping row are treated as wildcards.
    """
    best_row: Optional[dict[str, Any]] = None
    best_score = -1

    for row in mappings.item_rows:
        score = 0

        checks = [
            ("ignition_source_all", event.ignition_source_all_input),
            ("ignition_source_category", event.ignition_source_category),
            ("ignition_source", event.ignition_source),
            ("item_first_ignited", event.item_first_ignited_input),
            ("fire_start_location", event.room_of_origin_input),
            ("room_type", event.room_of_origin),
        ]

        matched = True
        for col, event_value in checks:
            raw_mapping_value = get_any(row, [col])
            mapping_key = normalise_lookup_key(raw_mapping_value)
            if not mapping_key:
                continue

            if mapping_key != normalise_lookup_key(event_value):
                matched = False
                break

            score += 1

        if matched and score > best_score:
            best_score = score
            best_row = row

    return best_row


# -----------------------------------------------------------------------------
# Final row quality
# -----------------------------------------------------------------------------

def _finalise_event_quality(event: PreparedFireEvent, warnings: list[FireEventWarning]) -> None:
    """
    Derive row-level data_quality_status and suspicious_fields.
    """
    if event.omit_from_model == "yes":
        event.data_quality_status = "omit"
        return

    if any(w.warning_severity in {"warning", "omit_row"} for w in warnings):
        if any(w.warning_severity == "warning" for w in warnings):
            event.data_quality_status = "warning"

    for warning in warnings:
        if warning.fire_parameter and warning.warning_severity in {"warning", "omit_row"}:
            event.suspicious_fields = _append_field(event.suspicious_fields, warning.fire_parameter)


def _append_field(existing: Optional[str], field_name: str) -> str:
    if not existing:
        return field_name
    fields = [x.strip() for x in existing.split("; ") if x.strip()]
    if field_name not in fields:
        fields.append(field_name)
    return "; ".join(fields)


# -----------------------------------------------------------------------------
# DB read helper
# -----------------------------------------------------------------------------

def _read_fris_staging_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """
    Read all FRIS staging rows.

    The current FRIS dataset is around 92k rows, which is small enough to hold in
    memory for this preparation stage.  If this grows much larger, this function
    can be replaced by a chunked generator without changing the route logic.
    """
    conn.row_factory = sqlite3.Row
    if TABLE_FRIS_STAGING not in _list_tables(conn):
        raise BlockingResolutionError(f"Missing FRIS staging table: {TABLE_FRIS_STAGING}")

    return list(conn.execute(f"SELECT * FROM {TABLE_FRIS_STAGING};").fetchall())


def _list_tables(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
    }
