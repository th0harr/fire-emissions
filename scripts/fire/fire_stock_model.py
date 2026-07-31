from __future__ import annotations

"""
Stage 1 affected-stock model for the Fire Emissions project.

Stage 1 does not calculate emitted CO2 or CO from combustion.  It only produces:
    1. direct affected carbon stock, kgC
    2. replacement embodied CO2, kg CO2

The later Stage 2 model will take the direct carbon rows from this table and
apply combustion/emission parameters.
"""

from typing import Any, Optional

from scripts.fire.fire_area_model import (
    FractionResult,
    capped_area,
    fraction_from_area,
    parse_area_band_estimate,
    parse_area_band_position,
)
from scripts.fire.fire_model_io import get_parameter_value
from scripts.fire.fire_model_records import (
    CarbonStockEstimate,
    EmbodiedCO2Estimate,
    FireModelWarning,
    ItemStockRecord,
    RoomStockRecord,
    Stage1ComponentResult,
)


# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------

ESTIMATE_CASES = ["low", "default", "high"]

MODELLED_SINGLE_ITEM_STATUSES = {
    "direct_inventory_item",
    "proxy_inventory_item",
    "conditionally_inferred_item",
}

# Older drafts used heat_smoke.  Current FRIS-facing schema uses
# heat_smoke_damage_only.  The model normalises both to one route.
HEAT_SMOKE_ALIASES = {"heat_smoke", "heat_smoke_damage_only"}


# Multiple-occupancy whole-dwelling fire scaling.
#
# First-pass conservative rule:
#   affected dwelling count = upper BFDA band area / 100 m2
#
# This is only applied to:
#   occupancy = multiple
#   fire_spread_category = entire_dwelling
#   building_fire_damage_area_band_index > 6
MULTIPLE_OCCUPANCY_AFFECTED_DWELLING_COUNT_BY_BFDA_INDEX = {
    7: 2,
    8: 5,
    9: 10,
    10: 20,
    11: 50,
}



# -----------------------------------------------------------------------------
# PUBLIC STAGE 1 ENTRY POINT
# -----------------------------------------------------------------------------

def build_stage1_component_results(
    *,
    events: list[dict[str, Any]],
    inventory_snapshot_id: int,
    room_lookup: dict[str, RoomStockRecord],
    item_lookup: dict[str, ItemStockRecord],
    dwelling_size_lookup: dict[str, Optional[float]],
    emission_parameters: dict[str, dict[str, dict[str, Optional[float]]]],
    whole_dwelling_carbon: CarbonStockEstimate,
    whole_dwelling_embodied: EmbodiedCO2Estimate,
    created_at_utc: str,
) -> tuple[list[Stage1ComponentResult], list[FireModelWarning]]:
    """
    Build all Stage 1 component rows from model-facing fire_events.

    This is the public Stage 1 entry point. It receives already-resolved
    fire_events rows, routes each event by fire_spread_category, and appends the
    category-specific affected-stock and replacement-embodied-CO2 component
    rows.

    Stage 1 responsibilities:
        - keep omitted fire_events out of model totals while recording a warning;
        - preserve the resolved reporting category on every output row;
        - calculate direct affected carbon stock in kgC;
        - calculate replacement embodied CO2 in kg CO2;
        - write structured warnings where inputs require capping, fallback logic,
          or cannot support a component calculation.

    Stage 1 deliberately does not calculate combustion-species emissions. Stage
    2 reads the direct carbon rows produced here and applies combustion,
    ventilation, and molecular-conversion parameters.
    """
    results: list[Stage1ComponentResult] = []
    warnings: list[FireModelWarning] = []

    for event in events:
        # Omitted rows are useful for coverage reporting but should not
        # contribute to the model output values.
        if str(event.get("omit_from_model", "no")).lower() == "yes":
            warnings.append(_warning(
                event=event,
                created_at_utc=created_at_utc,
                stage="stage_1_event_filter",
                warning_type="FIRE_MODEL_EVENT_OMITTED",
                warning_text=(
                    "Event is marked omit_from_model='yes' in fire_events and "
                    "has not contributed to Stage 1 outputs."
                ),
                raw_value=event.get("omit_reason"),
            ))
            continue

        category = normalise_fire_spread_category(event.get("fire_spread_category"))

        if category in HEAT_SMOKE_ALIASES:
            category_results, category_warnings = _build_heat_smoke_rows(
                event=event,
                inventory_snapshot_id=inventory_snapshot_id,
                created_at_utc=created_at_utc,
            )
        elif category == "single_item":
            category_results, category_warnings = _build_single_item_rows(
                event=event,
                inventory_snapshot_id=inventory_snapshot_id,
                room_lookup=room_lookup,
                item_lookup=item_lookup,
                emission_parameters=emission_parameters,
                created_at_utc=created_at_utc,
            )
        elif category == "within_room":
            category_results, category_warnings = _build_within_room_rows(
                event=event,
                inventory_snapshot_id=inventory_snapshot_id,
                room_lookup=room_lookup,
                emission_parameters=emission_parameters,
                created_at_utc=created_at_utc,
            )
        elif category == "multiple_rooms":
            category_results, category_warnings = _build_multiple_rooms_rows(
                event=event,
                inventory_snapshot_id=inventory_snapshot_id,
                room_lookup=room_lookup,
                dwelling_size_lookup=dwelling_size_lookup,
                emission_parameters=emission_parameters,
                whole_dwelling_carbon=whole_dwelling_carbon,
                whole_dwelling_embodied=whole_dwelling_embodied,
                created_at_utc=created_at_utc,
            )
        elif category == "entire_dwelling":
            category_results, category_warnings = _build_entire_dwelling_rows(
                event=event,
                inventory_snapshot_id=inventory_snapshot_id,
                whole_dwelling_carbon=whole_dwelling_carbon,
                whole_dwelling_embodied=whole_dwelling_embodied,
                created_at_utc=created_at_utc,
            )
        else:
            category_results = []
            category_warnings = [_warning(
                event=event,
                created_at_utc=created_at_utc,
                stage="stage_1_category_route",
                warning_type="UNMODELLED_FIRE_SPREAD_CATEGORY",
                warning_text=(
                    f"fire_spread_category={category!r} is not modelled in the "
                    "first-pass Stage 1 fire-emissions model."
                ),
                raw_value=event.get("fire_spread_category"),
                resolved_value=category,
            )]

        results.extend(category_results)
        warnings.extend(category_warnings)

    return results, warnings


# -----------------------------------------------------------------------------
# CATEGORY-SPECIFIC ROUTES
# -----------------------------------------------------------------------------

def _build_heat_smoke_rows(
    *,
    event: dict[str, Any],
    inventory_snapshot_id: int,
    created_at_utc: str,
) -> tuple[list[Stage1ComponentResult], list[FireModelWarning]]:
    """
    Build Stage 1 rows for heat/smoke-only events.

    Current resolution:
        - direct affected stock = 0
        - replacement embodied CO2 = 0

    This deliberately avoids using suspicious total-damage area entries for
    heat/smoke-only incidents.
    """
    rows: list[Stage1ComponentResult] = []
    warnings: list[FireModelWarning] = []

    for estimate_case in ESTIMATE_CASES:
        rows.append(_make_stage1_row(
            event=event,
            inventory_snapshot_id=inventory_snapshot_id,
            estimate_case=estimate_case,
            component_type="heat_smoke_damage_only",
            emission_pathway="none",
            area_basis="not_applicable",
            stock_basis="not_applicable",
            embodied_basis="assumed_zero_first_pass",
            direct_total_kgC=0.0,
            direct_biogenic_kgC=0.0,
            direct_fossil_kgC=0.0,
            replacement_embodied_CO2_kg=0.0,
            calculation_status="ok",
            calculation_notes=(
                "Heat/smoke-only event: direct combustion and replacement "
                "embodied CO2 are assumed zero in the first-pass model."
            ),
            created_at_utc=created_at_utc,
        ))

    if event.get("building_total_damage_area_input") not in (None, "", "None"):
        warnings.append(_warning(
            event=event,
            created_at_utc=created_at_utc,
            stage="stage_1_replacement_embodied",
            warning_type="HEAT_SMOKE_DAMAGE_AREA_IGNORED",
            warning_text=(
                "Heat/smoke-only event has a recorded total damage area, but "
                "the first-pass model assumes zero replacement embodied CO2 "
                "for this category."
            ),
            fire_parameter="building_total_damage_area_input",
            raw_value=event.get("building_total_damage_area_input"),
            resolved_value="replacement_embodied_CO2_kg=0",
        ))

    return rows, warnings


def _build_single_item_rows(
    *,
    event: dict[str, Any],
    inventory_snapshot_id: int,
    room_lookup: dict[str, RoomStockRecord],
    item_lookup: dict[str, ItemStockRecord],
    emission_parameters: dict[str, dict[str, dict[str, Optional[float]]]],
    created_at_utc: str,
) -> tuple[list[Stage1ComponentResult], list[FireModelWarning]]:
    """
    Build Stage 1 rows for single-item fires.

    Direct pathway:
        Single-item events use the resolved item_combusted lookup where
        single_item_status indicates a modelled item. The item carbon stock is
        not area-scaled.

    Replacement pathway:
        The first-pass model optionally adds origin-room replacement embodied
        CO2 using additional_replacement_factor. This represents small
        secondary replacement around the item rather than whole-room replacement.

    Invalid or unresolved single-item rows are not estimated here. They are
    retained in the warning stream so reporting can distinguish excluded
    single-item contributions from zero-emission events.
    """
    rows: list[Stage1ComponentResult] = []
    warnings: list[FireModelWarning] = []

    status = event.get("single_item_status")
    item_name = event.get("item_combusted")

    if status not in MODELLED_SINGLE_ITEM_STATUSES:
        warnings.append(_warning(
            event=event,
            created_at_utc=created_at_utc,
            stage="stage_1_direct_stock",
            warning_type="INVALID_SINGLE_ITEM_IGNORED",
            warning_text=(
                "Single-item event does not have a modelled item status and "
                "has not contributed to Stage 1 outputs."
            ),
            fire_parameter="single_item_status",
            raw_value=status,
        ))
        return rows, warnings

    item = item_lookup.get(str(item_name)) if item_name is not None else None

    if item is None:
        warnings.append(_warning(
            event=event,
            created_at_utc=created_at_utc,
            stage="stage_1_direct_stock",
            warning_type="ITEM_STOCK_LOOKUP_FAILED",
            warning_text=(
                "Single-item event has a modelled item status, but item_combusted "
                "could not be found in v_inventory_item_carbon_lookup."
            ),
            fire_parameter="item_combusted",
            raw_value=item_name,
        ))
        return rows, warnings

    # Direct item carbon row.
    for estimate_case in ESTIMATE_CASES:
        rows.append(_make_stage1_row(
            event=event,
            inventory_snapshot_id=inventory_snapshot_id,
            estimate_case=estimate_case,
            component_type="single_item",
            emission_pathway="direct",
            area_basis="single_item_not_area_scaled",
            stock_basis="v_inventory_item_carbon_lookup.item_combusted",
            embodied_basis=None,
            direct_total_kgC=item.carbon.total(estimate_case),
            direct_biogenic_kgC=item.carbon.biogenic(estimate_case),
            direct_fossil_kgC=item.carbon.fossil(estimate_case),
            replacement_embodied_CO2_kg=0.0,
            calculation_status="ok",
            calculation_notes="Single-item direct stock uses item_combusted lookup.",
            created_at_utc=created_at_utc,
        ))

    # Optional replacement row.
    # For this first model, use additional_replacement_factor as a flat fraction
    # of the origin-room embodied CO2, capped naturally because the factor is
    # validated on [0, 1] during emissions parameter ingest.
    room = room_lookup.get(str(event.get("room_of_origin"))) if event.get("room_of_origin") else None

    if room is not None:
        for estimate_case in ESTIMATE_CASES:
            replacement_factor = get_parameter_value(
                emission_parameters,
                fire_spread_category="single_item",
                parameter_name="additional_replacement_factor",
                estimate_case=estimate_case,
                default=0.0,
            ) or 0.0

            embodied = room.embodied_CO2.multiply(replacement_factor)

            rows.append(_make_stage1_row(
                event=event,
                inventory_snapshot_id=inventory_snapshot_id,
                estimate_case=estimate_case,
                component_type="single_item_room_replacement",
                emission_pathway="replacement",
                area_basis="additional_replacement_factor",
                stock_basis=None,
                embodied_basis="inventory_room_snapshot.room_embodied_CO2",
                area_fraction=replacement_factor,
                room_damage_fraction=replacement_factor,
                direct_total_kgC=0.0,
                direct_biogenic_kgC=0.0,
                direct_fossil_kgC=0.0,
                replacement_embodied_CO2_kg=embodied.get(estimate_case),
                calculation_status="ok",
                calculation_notes=(
                    "Single-item replacement uses additional_replacement_factor "
                    "as a fraction of origin-room embodied CO2."
                ),
                created_at_utc=created_at_utc,
            ))
    else:
        warnings.append(_warning(
            event=event,
            created_at_utc=created_at_utc,
            stage="stage_1_replacement_embodied",
            warning_type="ROOM_STOCK_LOOKUP_FAILED",
            warning_text=(
                "Single-item replacement embodied CO2 was not calculated because "
                "room_of_origin was missing or not found in inventory_room_snapshot."
            ),
            fire_parameter="room_of_origin",
            raw_value=event.get("room_of_origin"),
        ))

    return rows, warnings


def _build_within_room_rows(
    *,
    event: dict[str, Any],
    inventory_snapshot_id: int,
    room_lookup: dict[str, RoomStockRecord],
    emission_parameters: dict[str, dict[str, dict[str, Optional[float]]]],
    created_at_utc: str,
) -> tuple[list[Stage1ComponentResult], list[FireModelWarning]]:
    """
    Build Stage 1 rows for within-room fires.

    Direct pathway:
        The origin-room carbon stock is multiplied by room_fire_fraction, where
        room_fire_fraction is resolved from fire damage area divided by the
        model room size.

    Replacement pathway:
        Origin-room embodied CO2 is multiplied by room_damage_fraction, where
        room_damage_fraction is resolved from total damage area divided by the
        model room size.

    Both fractions are capped to the origin room. Within-room fires do not
    allocate residual dwelling stock or residual dwelling replacement, even if
    the total damage area suggests broader smoke, heat, or water damage.
    """
    rows: list[Stage1ComponentResult] = []
    warnings: list[FireModelWarning] = []

    room = room_lookup.get(str(event.get("room_of_origin"))) if event.get("room_of_origin") else None

    if room is None:
        return rows, [_warning(
            event=event,
            created_at_utc=created_at_utc,
            stage="stage_1_stock_lookup",
            warning_type="ROOM_STOCK_LOOKUP_FAILED",
            warning_text="within_room event could not be modelled because room stock lookup failed.",
            fire_parameter="room_of_origin",
            raw_value=event.get("room_of_origin"),
        )]

    room_size = _resolve_room_size_m2(event=event, room=room)

    fire_area_est = parse_area_band_estimate(event.get("building_fire_damage_area_input"))
    total_area_est = parse_area_band_estimate(
        event.get("building_total_damage_area_for_model")
        or event.get("building_total_damage_area_input")
    )

    for estimate_case in ESTIMATE_CASES:
        fire_area = fire_area_est.get(estimate_case)
        total_area = total_area_est.get(estimate_case)

        fire_fraction_result = fraction_from_area(fire_area, room_size)
        room_fire_fraction = fire_fraction_result.fraction

        if room_fire_fraction is None:
            warnings.append(_warning(
                event=event,
                created_at_utc=created_at_utc,
                stage="stage_1_area_resolution",
                warning_type="ROOM_FIRE_FRACTION_FAILED",
                warning_text="Could not calculate room_fire_fraction for within_room event.",
                fire_parameter="building_fire_damage_area_input",
                raw_value=event.get("building_fire_damage_area_input"),
            ))
            continue

        if fire_fraction_result.capped:
            warnings.append(_warning(
                event=event,
                created_at_utc=created_at_utc,
                stage="stage_1_area_resolution",
                warning_type="FIRE_AREA_CAPPED_TO_ROOM_SIZE",
                warning_text="Fire damage area exceeded room size and was capped to full room involvement.",
                fire_parameter="building_fire_damage_area_input",
                raw_value=event.get("building_fire_damage_area_input"),
                resolved_value="room_fire_fraction=1",
            ))

        direct_stock = room.carbon.multiply(room_fire_fraction)

        rows.append(_make_stage1_row(
            event=event,
            inventory_snapshot_id=inventory_snapshot_id,
            estimate_case=estimate_case,
            component_type="origin_room",
            emission_pathway="direct",
            area_basis="building_fire_damage_area_input / room_of_origin_size_m2",
            stock_basis="inventory_room_snapshot.room_of_origin",
            embodied_basis=None,
            building_fire_damage_area_m2=fire_area,
            room_of_origin_size_m2=room_size,
            area_fraction=room_fire_fraction,
            room_fire_fraction=room_fire_fraction,
            direct_total_kgC=direct_stock.total(estimate_case),
            direct_biogenic_kgC=direct_stock.biogenic(estimate_case),
            direct_fossil_kgC=direct_stock.fossil(estimate_case),
            replacement_embodied_CO2_kg=0.0,
            calculation_status="ok",
            calculation_notes="within_room direct stock uses room_fire_fraction.",
            created_at_utc=created_at_utc,
        ))

        # Replacement is capped to the room, even when total damage looks larger.
        total_area_capped, was_capped = capped_area(total_area, room_size)
        damage_fraction_result = fraction_from_area(total_area_capped, room_size)
        room_damage_fraction = damage_fraction_result.fraction

        if room_damage_fraction is None:
            warnings.append(_warning(
                event=event,
                created_at_utc=created_at_utc,
                stage="stage_1_replacement_embodied",
                warning_type="ROOM_DAMAGE_FRACTION_FAILED",
                warning_text="Could not calculate room_damage_fraction for within_room replacement.",
                fire_parameter="building_total_damage_area_for_model",
                raw_value=event.get("building_total_damage_area_for_model"),
            ))
            continue

        if was_capped:
            warnings.append(_warning(
                event=event,
                created_at_utc=created_at_utc,
                stage="stage_1_replacement_embodied",
                warning_type="TOTAL_DAMAGE_AREA_CAPPED_TO_ROOM_SIZE",
                warning_text="Total damage area exceeded room size and was capped to origin-room replacement.",
                fire_parameter="building_total_damage_area_for_model",
                raw_value=event.get("building_total_damage_area_for_model"),
                resolved_value="room_damage_fraction=1",
            ))

        embodied = room.embodied_CO2.multiply(room_damage_fraction)

        rows.append(_make_stage1_row(
            event=event,
            inventory_snapshot_id=inventory_snapshot_id,
            estimate_case=estimate_case,
            component_type="origin_room",
            emission_pathway="replacement",
            area_basis="building_total_damage_area_for_model / room_of_origin_size_m2",
            stock_basis=None,
            embodied_basis="inventory_room_snapshot.room_embodied_CO2",
            building_total_damage_area_m2=total_area,
            replacement_damage_area_m2=total_area_capped,
            room_of_origin_size_m2=room_size,
            area_fraction=room_damage_fraction,
            room_damage_fraction=room_damage_fraction,
            direct_total_kgC=0.0,
            direct_biogenic_kgC=0.0,
            direct_fossil_kgC=0.0,
            replacement_embodied_CO2_kg=embodied.get(estimate_case),
            calculation_status="ok",
            calculation_notes="within_room replacement embodied CO2 is capped to the origin room.",
            created_at_utc=created_at_utc,
        ))

    return rows, warnings


def _build_multiple_rooms_rows(
    *,
    event: dict[str, Any],
    inventory_snapshot_id: int,
    room_lookup: dict[str, RoomStockRecord],
    dwelling_size_lookup: dict[str, Optional[float]],
    emission_parameters: dict[str, dict[str, dict[str, Optional[float]]]],
    whole_dwelling_carbon: CarbonStockEstimate,
    whole_dwelling_embodied: EmbodiedCO2Estimate,
    created_at_utc: str,
) -> tuple[list[Stage1ComponentResult], list[FireModelWarning]]:
    """
    Build Stage 1 rows for multiple-room fires.

    Ordinary route:
        Multiple-room fires are modelled as:

            1. a full origin-room direct carbon component;
            2. a residual-dwelling direct carbon component;
            3. a whole-dwelling replacement embodied-CO2 component.

        The residual direct component is based on the fire area remaining after
        the origin-room size is subtracted. The replacement component is based
        on total damage area divided by dwelling size and capped to the full
        dwelling.

    Same-band residual fallback:
        If the ordinary residual calculation gives no positive residual area
        and the controlled area-band indices show:

            building_room_origin_size_band_index
                == building_fire_damage_area_band_index

        the model applies a q25/q75 within-band fallback. This represents the
        case where both values fall in the same broad FRIS band, but the event
        category still indicates spread beyond the origin room.

    Area-limited fallback:
        If the origin-room size band is higher than the fire-damage area band,
        the quantitative area data do not support either full origin-room
        involvement or a residual-dwelling component. The event is retained as
        fire_spread_category = multiple_rooms for reporting, but Stage 1 uses a
        within_room-style area-limited calculation for direct stock and
        replacement embodied CO2.

    The function deliberately avoids omitting these contradictory records. It
    instead writes explicit warnings so Stage 3 can report how many events used
    each modelling route.
    """
    rows: list[Stage1ComponentResult] = []
    warnings: list[FireModelWarning] = []

    room = room_lookup.get(str(event.get("room_of_origin"))) if event.get("room_of_origin") else None

    if room is None:
        return rows, [_warning(
            event=event,
            created_at_utc=created_at_utc,
            stage="stage_1_stock_lookup",
            warning_type="ROOM_STOCK_LOOKUP_FAILED",
            warning_text="multiple_rooms event could not be modelled because origin room stock lookup failed.",
            fire_parameter="room_of_origin",
            raw_value=event.get("room_of_origin"),
        )]

    room_size = _resolve_room_size_m2(event=event, room=room)
    dwelling_size = _resolve_dwelling_size_m2(event=event, dwelling_size_lookup=dwelling_size_lookup)

    fire_area_est = parse_area_band_estimate(event.get("building_fire_damage_area_input"))
    total_area_est = parse_area_band_estimate(
        event.get("building_total_damage_area_for_model")
        or event.get("building_total_damage_area_input")
    )

    (
        area_band_relationship,
        room_size_band_index,
        fire_damage_band_index,
    ) = _multiple_rooms_area_band_relationship(event=event)

    # Contradictory band order:
    #
    #     room-origin size band > fire-damage area band
    #
    # The event remains multiple_rooms for reporting, but the available area
    # data only support an area-limited, within-room-style calculation.
    if area_band_relationship == "room_origin_band_higher_than_fire_area_band":
        for estimate_case in ESTIMATE_CASES:
            fire_area = fire_area_est.get(estimate_case)
            total_area = total_area_est.get(estimate_case)

            fallback_rows, fallback_warnings = _build_multiple_rooms_area_limited_rows(
                event=event,
                inventory_snapshot_id=inventory_snapshot_id,
                estimate_case=estimate_case,
                created_at_utc=created_at_utc,
                room=room,
                fire_area_m2=fire_area,
                total_area_m2=total_area,
                room_size_m2=room_size,
                area_band_relationship=area_band_relationship,
                room_band_index=room_size_band_index,
                fire_band_index=fire_damage_band_index,
            )

            rows.extend(fallback_rows)
            warnings.extend(fallback_warnings)

        return rows, warnings

    # Normal multiple_rooms route starts here.  This is deliberately after the
    # area-limited fallback branch, because contradictory band-order events do
    # not use a residual dwelling carbon component.
    residual_carbon = whole_dwelling_carbon.subtract(room.carbon)

    for estimate_case in ESTIMATE_CASES:
        fire_area = fire_area_est.get(estimate_case)
        total_area = total_area_est.get(estimate_case)

        # Component 1: full origin-room direct stock.
        rows.append(_make_stage1_row(
            event=event,
            inventory_snapshot_id=inventory_snapshot_id,
            estimate_case=estimate_case,
            component_type="origin_room",
            emission_pathway="direct",
            area_basis="full_origin_room_multiple_rooms_assumption",
            stock_basis="inventory_room_snapshot.room_of_origin",
            embodied_basis=None,
            building_fire_damage_area_m2=fire_area,
            room_of_origin_size_m2=room_size,
            area_fraction=1.0,
            room_fire_fraction=1.0,
            direct_total_kgC=room.carbon.total(estimate_case),
            direct_biogenic_kgC=room.carbon.biogenic(estimate_case),
            direct_fossil_kgC=room.carbon.fossil(estimate_case),
            replacement_embodied_CO2_kg=0.0,
            calculation_status="ok",
            calculation_notes="multiple_rooms direct stock assumes full origin-room involvement.",
            created_at_utc=created_at_utc,
        ))

        # Component 2: residual dwelling direct stock.
        (
            residual_fire_area,
            residual_dwelling_area,
            residual_fraction_result,
            residual_quartile_fallback_used,
        ) = _resolve_multiple_rooms_residual_fraction(
            event=event,
            fire_area_m2=fire_area,
            room_size_m2=room_size,
            dwelling_size_m2=dwelling_size,
        )

        residual_fire_fraction = residual_fraction_result.fraction

        if residual_quartile_fallback_used:
            warnings.append(_warning(
                event=event,
                created_at_utc=created_at_utc,
                stage="stage_1_area_resolution",
                warning_type="MULTIPLE_ROOMS_RESIDUAL_AREA_QUARTILE_FALLBACK_USED",
                warning_text=(
                    "Residual dwelling fire fraction was resolved using a "
                    "quartile-based area-band fallback for a multiple_rooms event."
                ),
                fire_parameter="building_room_origin_size_input; building_fire_damage_area_input",
                raw_value=(
                    f"{event.get('building_room_origin_size_input')}; "
                    f"{event.get('building_fire_damage_area_input')}"
                ),
                resolved_value=f"residual_fire_fraction={residual_fire_fraction}",
            ))

        if residual_fire_fraction is None or residual_fire_fraction <= 0.0:
            residual_fire_fraction = 0.0
            warnings.append(_warning(
                event=event,
                created_at_utc=created_at_utc,
                stage="stage_1_area_resolution",
                warning_type="RESIDUAL_FIRE_FRACTION_FAILED",
                warning_text=(
                    "Residual dwelling fire fraction could not be calculated. "
                    "For same-band multiple_rooms cases, the quartile fallback "
                    "was attempted before setting the residual direct component "
                    "to zero. Cases where the origin-room size band exceeded "
                    "the fire-damage area band should use the area-limited "
                    "within-room fallback instead."
                ),
                fire_parameter=(
                    "building_room_origin_size_band_index; "
                    "building_fire_damage_area_band_index"
                ),
                raw_value=(
                    f"{event.get('building_room_origin_size_input')}; "
                    f"{event.get('building_fire_damage_area_input')}"
                ),
                resolved_value=(
                    f"area_band_relationship={area_band_relationship}; "
                    f"room_band_index={room_size_band_index}; "
                    f"fire_band_index={fire_damage_band_index}"
                ),
            ))

        residual_direct = residual_carbon.multiply(residual_fire_fraction)

        rows.append(_make_stage1_row(
            event=event,
            inventory_snapshot_id=inventory_snapshot_id,
            estimate_case=estimate_case,
            component_type="residual_dwelling",
            emission_pathway="direct",
            area_basis=(
                "quartile_fallback_residual_fire_area_m2 / residual_dwelling_area_m2"
                if residual_quartile_fallback_used
                else "residual_fire_area_m2 / residual_dwelling_area_m2"
            ),
            stock_basis="sum(inventory_room_snapshot) - origin_room",
            embodied_basis=None,
            building_fire_damage_area_m2=fire_area,
            residual_fire_area_m2=residual_fire_area,
            residual_dwelling_area_m2=residual_dwelling_area,
            room_of_origin_size_m2=room_size,
            dwelling_size_m2=dwelling_size,
            area_fraction=residual_fire_fraction,
            residual_fire_fraction=residual_fire_fraction,
            direct_total_kgC=residual_direct.total(estimate_case),
            direct_biogenic_kgC=residual_direct.biogenic(estimate_case),
            direct_fossil_kgC=residual_direct.fossil(estimate_case),
            replacement_embodied_CO2_kg=0.0,
            calculation_status="ok",
            calculation_notes=(
                "multiple_rooms residual direct stock uses quartile fallback "
                "for residual_fire_fraction."
                if residual_quartile_fallback_used
                else "multiple_rooms residual direct stock uses residual_fire_fraction."
            ),
            created_at_utc=created_at_utc,
        ))

        # Replacement pathway: use total damage fraction over the full dwelling.
        (
            total_area_capped,
            damage_fraction_result,
            was_capped,
            damage_quartile_fallback_used,
            dwelling_damage_area_basis,
        ) = _resolve_multiple_rooms_dwelling_damage_fraction(
            event=event,
            total_area_m2=total_area,
            fire_area_m2=fire_area,
            dwelling_size_m2=dwelling_size,
        )

        dwelling_damage_fraction = damage_fraction_result.fraction

        if damage_quartile_fallback_used:
            warnings.append(_warning(
                event=event,
                created_at_utc=created_at_utc,
                stage="stage_1_replacement_embodied",
                warning_type="MULTIPLE_ROOMS_DWELLING_DAMAGE_QUARTILE_FALLBACK_USED",
                warning_text=(
                    "Dwelling damage fraction was resolved using a quartile-based "
                    "area-band fallback for a multiple_rooms event."
                ),
                fire_parameter="building_total_damage_area_for_model; building_fire_damage_area_input",
                raw_value=(
                    f"{event.get('building_total_damage_area_for_model') or event.get('building_total_damage_area_input')}; "
                    f"{event.get('building_fire_damage_area_input')}"
                ),
                resolved_value=f"dwelling_damage_fraction={dwelling_damage_fraction}",
            ))

        if dwelling_damage_fraction is None or dwelling_damage_fraction <= 0.0:
            dwelling_damage_fraction = 0.0
            warnings.append(_warning(
                event=event,
                created_at_utc=created_at_utc,
                stage="stage_1_replacement_embodied",
                warning_type="DWELLING_DAMAGE_FRACTION_FAILED",
                warning_text=(
                    "Dwelling damage fraction could not be calculated, even "
                    "after the multiple_rooms quartile fallback. Replacement "
                    "embodied CO2 has been set to zero."
                ),
                fire_parameter="building_total_damage_area_for_model",
                raw_value=event.get("building_total_damage_area_for_model"),
            ))

        if was_capped:
            warnings.append(_warning(
                event=event,
                created_at_utc=created_at_utc,
                stage="stage_1_replacement_embodied",
                warning_type="TOTAL_DAMAGE_AREA_CAPPED_TO_DWELLING_SIZE",
                warning_text="Total damage area exceeded dwelling size and was capped to full dwelling replacement.",
                fire_parameter="building_total_damage_area_for_model",
                raw_value=event.get("building_total_damage_area_for_model"),
                resolved_value="dwelling_damage_fraction=1",
            ))

        embodied = whole_dwelling_embodied.multiply(dwelling_damage_fraction)

        rows.append(_make_stage1_row(
            event=event,
            inventory_snapshot_id=inventory_snapshot_id,
            estimate_case=estimate_case,
            component_type="whole_dwelling_damage",
            emission_pathway="replacement",
            area_basis=dwelling_damage_area_basis,
            stock_basis=None,
            embodied_basis="sum(inventory_room_snapshot.room_embodied_CO2)",
            building_total_damage_area_m2=total_area,
            replacement_damage_area_m2=total_area_capped,
            dwelling_size_m2=dwelling_size,
            area_fraction=dwelling_damage_fraction,
            dwelling_damage_fraction=dwelling_damage_fraction,
            direct_total_kgC=0.0,
            direct_biogenic_kgC=0.0,
            direct_fossil_kgC=0.0,
            replacement_embodied_CO2_kg=embodied.get(estimate_case),
            calculation_status="ok",
            calculation_notes=(
                "multiple_rooms replacement embodied CO2 uses quartile fallback "
                "for dwelling_damage_fraction."
                if damage_quartile_fallback_used
                else "multiple_rooms replacement embodied CO2 uses total dwelling damage fraction."
            ),
            created_at_utc=created_at_utc,
        ))

    return rows, warnings

def _build_multiple_rooms_area_limited_rows(
    *,
    event: dict[str, Any],
    inventory_snapshot_id: int,
    estimate_case: str,
    created_at_utc: str,
    room: RoomStockRecord,
    fire_area_m2: Optional[float],
    total_area_m2: Optional[float],
    room_size_m2: Optional[float],
    area_band_relationship: str,
    room_band_index: Optional[int],
    fire_band_index: Optional[int],
) -> tuple[list[Stage1ComponentResult], list[FireModelWarning]]:
    """
    Build area-limited fallback rows for contradictory multiple_rooms events.

    This route is used when the resolved event category is multiple_rooms, but:

        building_room_origin_size_band_index
            > building_fire_damage_area_band_index

    Modelling interpretation:
        The event is retained as multiple_rooms for reporting. However, the
        quantitative area fields do not support either full origin-room
        involvement or any residual dwelling component. For those cases, Stage 1
        uses a within_room-style calculation while preserving the original
        multiple_rooms category on the output rows.

    Direct pathway:
        origin-room carbon stock × room_fire_fraction

        where room_fire_fraction is based on fire damage area divided by the
        model origin-room size.

    Replacement pathway:
        origin-room embodied CO2 × room_damage_fraction

        where room_damage_fraction is based on total damage area divided by the
        model origin-room size. If total damage area is unavailable or zero, the
        fire damage area is used as a conservative replacement proxy.

    This function does not create:
        - a full-origin-room direct component;
        - a residual-dwelling direct component;
        - a whole-dwelling replacement component.
    """
    rows: list[Stage1ComponentResult] = []
    warnings: list[FireModelWarning] = []

    warnings.append(_warning(
        event=event,
        created_at_utc=created_at_utc,
        stage="stage_1_area_resolution",
        warning_type="MULTIPLE_ROOMS_AREA_LIMITED_WITHIN_ROOM_FALLBACK_USED",
        warning_text=(
            "Event is classified as multiple_rooms, but the origin-room size "
            "band index is higher than the fire-damage area band index. The "
            "event was retained as multiple_rooms for reporting, but Stage 1 "
            "used a within_room-style area-limited calculation."
        ),
        fire_parameter=(
            "building_room_origin_size_band_index; "
            "building_fire_damage_area_band_index"
        ),
        raw_value=(
            f"{event.get('building_room_origin_size_input')}; "
            f"{event.get('building_fire_damage_area_input')}"
        ),
        resolved_value=(
            f"area_band_relationship={area_band_relationship}; "
            f"room_band_index={room_band_index}; "
            f"fire_band_index={fire_band_index}"
        ),
    ))

    # Direct pathway: fire area fraction over the origin room.
    fire_area_capped, fire_area_was_capped = capped_area(
        fire_area_m2,
        room_size_m2,
    )

    room_fire_fraction_result = fraction_from_area(
        fire_area_capped,
        room_size_m2,
    )

    room_fire_fraction = room_fire_fraction_result.fraction

    if room_fire_fraction is None:
        room_fire_fraction = 0.0
        warnings.append(_warning(
            event=event,
            created_at_utc=created_at_utc,
            stage="stage_1_area_resolution",
            warning_type="ROOM_FIRE_FRACTION_FAILED",
            warning_text=(
                "Room fire fraction could not be calculated for the "
                "multiple_rooms area-limited fallback. Direct affected stock "
                "has been set to zero."
            ),
            fire_parameter="building_fire_damage_area_input; building_room_origin_size_input",
            raw_value=(
                f"{event.get('building_fire_damage_area_input')}; "
                f"{event.get('building_room_origin_size_input')}"
            ),
            resolved_value=(
                f"area_band_relationship={area_band_relationship}; "
                f"room_band_index={room_band_index}; "
                f"fire_band_index={fire_band_index}"
            ),
        ))

    if fire_area_was_capped:
        warnings.append(_warning(
            event=event,
            created_at_utc=created_at_utc,
            stage="stage_1_area_resolution",
            warning_type="FIRE_AREA_CAPPED_TO_ROOM_SIZE",
            warning_text=(
                "Fire damage area exceeded origin-room size and was capped "
                "during the multiple_rooms area-limited fallback."
            ),
            fire_parameter="building_fire_damage_area_input",
            raw_value=event.get("building_fire_damage_area_input"),
            resolved_value="room_fire_fraction=1",
        ))

    direct_stock = room.carbon.multiply(room_fire_fraction)

    rows.append(_make_stage1_row(
        event=event,
        inventory_snapshot_id=inventory_snapshot_id,
        estimate_case=estimate_case,
        component_type="origin_room",
        emission_pathway="direct",
        calculation_status="ok",
        stock_basis=(
            "origin room carbon stock × room_fire_fraction; "
            "multiple_rooms area-limited fallback"
        ),
        area_basis="building_fire_damage_area_for_model / building_room_origin_size",
        embodied_basis=None,
        building_fire_damage_area_m2=fire_area_m2,
        room_of_origin_size_m2=room_size_m2,
        area_fraction=room_fire_fraction,
        room_fire_fraction=room_fire_fraction,
        residual_fire_fraction=None,
        dwelling_damage_fraction=None,
        affected_dwelling_count=1,
        direct_total_kgC=direct_stock.total(estimate_case),
        direct_biogenic_kgC=direct_stock.biogenic(estimate_case),
        direct_fossil_kgC=direct_stock.fossil(estimate_case),
        replacement_embodied_CO2_kg=0.0,
        calculation_notes=(
            "multiple_rooms retained for reporting; direct stock calculated "
            "using within_room-style area-limited fallback because origin-room "
            "size band exceeded fire-damage area band."
        ),
        created_at_utc=created_at_utc,
    ))

    # Replacement pathway: total damage fraction over the origin room.
    #
    # If total damage area is unavailable, use fire damage area as a conservative
    # proxy, because this fallback is explicitly area-limited to the origin room.
    replacement_area_m2 = total_area_m2
    replacement_area_basis = (
        "building_total_damage_area_for_model / building_room_origin_size"
    )

    if replacement_area_m2 is None or replacement_area_m2 <= 0.0:
        replacement_area_m2 = fire_area_m2
        replacement_area_basis = (
            "building_fire_damage_area_for_model / building_room_origin_size"
        )

        warnings.append(_warning(
            event=event,
            created_at_utc=created_at_utc,
            stage="stage_1_replacement_embodied",
            warning_type=(
                "MULTIPLE_ROOMS_AREA_LIMITED_REPLACEMENT_FIRE_AREA_PROXY_USED"
            ),
            warning_text=(
                "Total damage area was unavailable or zero for the "
                "multiple_rooms area-limited fallback. Fire damage area was "
                "used as the origin-room replacement damage proxy."
            ),
            fire_parameter="building_total_damage_area_for_model; building_fire_damage_area_input",
            raw_value=(
                f"{event.get('building_total_damage_area_for_model')}; "
                f"{event.get('building_fire_damage_area_input')}"
            ),
            resolved_value=replacement_area_basis,
        ))

    replacement_area_capped, replacement_area_was_capped = capped_area(
        replacement_area_m2,
        room_size_m2,
    )

    room_damage_fraction_result = fraction_from_area(
        replacement_area_capped,
        room_size_m2,
    )

    room_damage_fraction = room_damage_fraction_result.fraction

    if room_damage_fraction is None:
        room_damage_fraction = 0.0
        warnings.append(_warning(
            event=event,
            created_at_utc=created_at_utc,
            stage="stage_1_replacement_embodied",
            warning_type="ROOM_DAMAGE_FRACTION_FAILED",
            warning_text=(
                "Room damage fraction could not be calculated for the "
                "multiple_rooms area-limited fallback. Replacement embodied "
                "CO2 has been set to zero."
            ),
            fire_parameter="building_total_damage_area_for_model; building_room_origin_size_input",
            raw_value=(
                f"{event.get('building_total_damage_area_for_model')}; "
                f"{event.get('building_room_origin_size_input')}"
            ),
            resolved_value=(
                f"area_band_relationship={area_band_relationship}; "
                f"room_band_index={room_band_index}; "
                f"fire_band_index={fire_band_index}"
            ),
        ))

    if replacement_area_was_capped:
        warnings.append(_warning(
            event=event,
            created_at_utc=created_at_utc,
            stage="stage_1_replacement_embodied",
            warning_type="TOTAL_DAMAGE_AREA_CAPPED_TO_ROOM_SIZE",
            warning_text=(
                "Replacement damage area exceeded origin-room size and was "
                "capped during the multiple_rooms area-limited fallback."
            ),
            fire_parameter="building_total_damage_area_for_model",
            raw_value=event.get("building_total_damage_area_for_model"),
            resolved_value="room_damage_fraction=1",
        ))

    embodied = room.embodied_CO2.multiply(room_damage_fraction)

    rows.append(_make_stage1_row(
        event=event,
        inventory_snapshot_id=inventory_snapshot_id,
        estimate_case=estimate_case,
        component_type="origin_room",
        emission_pathway="replacement",
        calculation_status="ok",
        stock_basis=None,
        area_basis=replacement_area_basis,
        embodied_basis=(
            "origin room embodied CO2 × room_damage_fraction; "
            "multiple_rooms area-limited fallback"
        ),
        building_fire_damage_area_m2=fire_area_m2,
        building_total_damage_area_m2=total_area_m2,
        replacement_damage_area_m2=replacement_area_capped,
        room_of_origin_size_m2=room_size_m2,
        area_fraction=room_damage_fraction,
        room_fire_fraction=None,
        room_damage_fraction=room_damage_fraction,
        residual_fire_fraction=None,
        dwelling_damage_fraction=None,
        affected_dwelling_count=1,
        direct_total_kgC=0.0,
        direct_biogenic_kgC=0.0,
        direct_fossil_kgC=0.0,
        replacement_embodied_CO2_kg=embodied.get(estimate_case),
        calculation_notes=(
            "multiple_rooms retained for reporting; replacement embodied CO2 "
            "calculated using within_room-style area-limited fallback because "
            "origin-room size band exceeded fire-damage area band."
        ),
        created_at_utc=created_at_utc,
    ))

    return rows, warnings

def _build_entire_dwelling_rows(
    *,
    event: dict[str, Any],
    inventory_snapshot_id: int,
    whole_dwelling_carbon: CarbonStockEstimate,
    whole_dwelling_embodied: EmbodiedCO2Estimate,
    created_at_utc: str,
) -> tuple[list[Stage1ComponentResult], list[FireModelWarning]]:
    """
    Build Stage 1 rows for whole-dwelling fires.

    Ordinary entire-dwelling fires are modelled as one affected dwelling.

    For large multiple-occupancy entire-dwelling fires, FRIS may record a fire
    affecting multiple dwelling units as a single incident. In those cases, the
    model estimates a total affected dwelling count from the building fire
    damage area band and uses it as a multiplier for both:

        - direct whole-dwelling carbon stock;
        - replacement whole-dwelling embodied CO2.

    This is a deliberately simple first-pass assumption.
    """
    rows: list[Stage1ComponentResult] = []
    warnings: list[FireModelWarning] = []

    affected_dwelling_count = _resolve_affected_dwelling_count(event=event)

    if affected_dwelling_count > 1:
        warnings.append(_warning(
            event=event,
            created_at_utc=created_at_utc,
            stage="stage_1_stock_scaling",
            warning_type="MULTIPLE_OCCUPANCY_AFFECTED_DWELLING_COUNT_ESTIMATED",
            warning_text=(
                "Multiple-occupancy entire-dwelling fire has building fire "
                "damage area above the 51-100 m2 band. The model has estimated "
                "the total number of affected dwellings from the fire damage "
                "area band and scaled whole-dwelling stock accordingly."
            ),
            fire_parameter="building_fire_damage_area_band_index",
            raw_value=event.get("building_fire_damage_area_band_index"),
            resolved_value=f"affected_dwelling_count={affected_dwelling_count}",
        ))

    for estimate_case in ESTIMATE_CASES:
        rows.append(_make_stage1_row(
            event=event,
            inventory_snapshot_id=inventory_snapshot_id,
            estimate_case=estimate_case,
            component_type="whole_dwelling",
            emission_pathway="direct",
            area_basis="entire_dwelling_category",
            stock_basis="sum(inventory_room_snapshot) * affected_dwelling_count",
            embodied_basis=None,
            area_fraction=1.0,
            dwelling_damage_fraction=1.0,
            affected_dwelling_count=affected_dwelling_count,
            direct_total_kgC=_scale_optional(
                whole_dwelling_carbon.total(estimate_case),
                affected_dwelling_count,
            ),
            direct_biogenic_kgC=_scale_optional(
                whole_dwelling_carbon.biogenic(estimate_case),
                affected_dwelling_count,
            ),
            direct_fossil_kgC=_scale_optional(
                whole_dwelling_carbon.fossil(estimate_case),
                affected_dwelling_count,
            ),
            replacement_embodied_CO2_kg=0.0,
            calculation_status="ok",
            calculation_notes=(
                "entire_dwelling direct stock uses whole dwelling stock scaled "
                "by affected_dwelling_count."
            ),
            created_at_utc=created_at_utc,
        ))

        rows.append(_make_stage1_row(
            event=event,
            inventory_snapshot_id=inventory_snapshot_id,
            estimate_case=estimate_case,
            component_type="whole_dwelling",
            emission_pathway="replacement",
            area_basis="entire_dwelling_category",
            stock_basis=None,
            embodied_basis=(
                "sum(inventory_room_snapshot.room_embodied_CO2) "
                "* affected_dwelling_count"
            ),
            area_fraction=1.0,
            dwelling_damage_fraction=1.0,
            affected_dwelling_count=affected_dwelling_count,
            direct_total_kgC=0.0,
            direct_biogenic_kgC=0.0,
            direct_fossil_kgC=0.0,
            replacement_embodied_CO2_kg=_scale_optional(
                whole_dwelling_embodied.get(estimate_case),
                affected_dwelling_count,
            ),
            calculation_status="ok",
            calculation_notes=(
                "entire_dwelling replacement embodied CO2 uses whole dwelling "
                "embodied CO2 scaled by affected_dwelling_count."
            ),
            created_at_utc=created_at_utc,
        ))

    return rows, warnings


# -----------------------------------------------------------------------------
# COMMON ROW / WARNING HELPERS
# -----------------------------------------------------------------------------

def normalise_fire_spread_category(value: object) -> str:
    """
    Normalise fire_spread_category values used by older and newer code.
    """
    if value is None:
        return "unspecified"

    text = str(value).strip().lower()

    if text == "heat_smoke":
        return "heat_smoke_damage_only"

    return text


def _resolve_room_size_m2(*, event: dict[str, Any], room: RoomStockRecord) -> Optional[float]:
    """
    Resolve room size.

    Current fire_events store the original FRIS size band, while
    inventory_room_snapshot stores a model room size.  For Stage 1 we use the
    inventory room size as the denominator for room fractions.
    """
    return room.room_size_m2


def _resolve_dwelling_size_m2(
    *,
    event: dict[str, Any],
    dwelling_size_lookup: dict[str, Optional[float]],
) -> Optional[float]:
    """
    Resolve dwelling size from dwelling_type_for_model first, then dwelling_type.
    """
    dwelling_type = event.get("dwelling_type_for_model") or event.get("dwelling_type")

    if dwelling_type is None:
        return None

    return dwelling_size_lookup.get(str(dwelling_type))


def _resolve_affected_dwelling_count(*, event: dict[str, Any]) -> int:
    """
    Resolve the total number of dwellings represented by one Stage 1 event.

    Default:
        1 affected dwelling.

    Multiple-occupancy large-fire rule:
        If the event is a multiple-occupancy entire-dwelling fire and the
        building fire damage area band is above the 51-100 m2 band, estimate
        affected dwellings from the area band upper range.

    Note: This is intentionally conservative. 
    It avoids treating large block fires as a single dwelling fire, 
    but may still underestimate affected dwellings in high-density buildings.
    """
    occupancy = str(event.get("occupancy") or "").strip().lower()
    fire_spread_category = normalise_fire_spread_category(
        event.get("fire_spread_category")
    )

    if occupancy != "multiple":
        return 1

    if fire_spread_category != "entire_dwelling":
        return 1

    bfda_index = _to_int_or_none(
        event.get("building_fire_damage_area_band_index")
    )

    if bfda_index is None:
        return 1

    if bfda_index <= 6:
        return 1

    if bfda_index in MULTIPLE_OCCUPANCY_AFFECTED_DWELLING_COUNT_BY_BFDA_INDEX:
        return MULTIPLE_OCCUPANCY_AFFECTED_DWELLING_COUNT_BY_BFDA_INDEX[bfda_index]

    # Defensive fallback for any larger/open-ended band not explicitly mapped.
    return max(MULTIPLE_OCCUPANCY_AFFECTED_DWELLING_COUNT_BY_BFDA_INDEX.values())


def _multiple_rooms_area_band_relationship(
    *,
    event: dict[str, Any],
) -> tuple[str, Optional[int], Optional[int]]:
    """
    Compare origin-room size band and fire-damage area band for multiple_rooms.

    Returns
    -------
    tuple
        relationship,
        room_band_index,
        fire_band_index

    relationship values:
        missing_band_index
        room_origin_band_lower_than_fire_area_band
        same_band
        room_origin_band_higher_than_fire_area_band

    Notes
    -----
    For ordinary multiple_rooms modelling, the fire-damage area should be in
    the same or a higher area band than the origin-room size.

    If the origin-room size band is higher than the fire-damage area band, the
    quantitative area data do not support a residual-dwelling fire area. These
    events are retained as multiple_rooms for reporting, but are modelled using
    an area-limited within-room-style fallback.
    """
    room_band_index = _to_int_or_none(
        event.get("building_room_origin_size_band_index")
    )

    fire_band_index = _to_int_or_none(
        event.get("building_fire_damage_area_band_index")
    )

    if room_band_index is None or fire_band_index is None:
        return "missing_band_index", room_band_index, fire_band_index

    if room_band_index < fire_band_index:
        return (
            "room_origin_band_lower_than_fire_area_band",
            room_band_index,
            fire_band_index,
        )

    if room_band_index == fire_band_index:
        return "same_band", room_band_index, fire_band_index

    return (
        "room_origin_band_higher_than_fire_area_band",
        room_band_index,
        fire_band_index,
    )


def _to_int_or_none(value: object) -> Optional[int]:
    """
    Convert a database value to int, preserving None / blank as None.
    """
    if value is None:
        return None

    text = str(value).strip()

    if text == "":
        return None

    return int(float(text))


def _scale_optional(value: Optional[float], factor: int | float) -> Optional[float]:
    """
    Multiply a possibly missing numeric value.
    """
    if value is None:
        return None

    return value * float(factor)


def _resolve_multiple_rooms_residual_fraction(
    *,
    event: dict[str, Any],
    fire_area_m2: Optional[float],
    room_size_m2: Optional[float],
    dwelling_size_m2: Optional[float],
) -> tuple[Optional[float], Optional[float], FractionResult, bool]:
    """
    Resolve residual dwelling fire fraction for a multiple_rooms event.

    Ordinary calculation:
        residual_fire_area = fire_area_m2 - room_size_m2
        residual_dwelling_area = dwelling_size_m2 - room_size_m2

    Quartile fallback:
        If the ordinary calculation fails or gives no positive residual, use a
        q25/q75 same-band fallback.

        This fallback is only valid when:

            building_room_origin_size_band_index
                == building_fire_damage_area_band_index

        In that case, the area bands are ambiguous: the fire may have spread
        beyond the origin room even though both values fall in the same broad
        FRIS area band.
    """
    residual_fire_area = None
    residual_dwelling_area = None

    # Ordinary residual calculation.
    if fire_area_m2 is not None and room_size_m2 is not None:
        residual_fire_area = max(fire_area_m2 - room_size_m2, 0.0)

    if dwelling_size_m2 is not None and room_size_m2 is not None:
        residual_dwelling_area = max(dwelling_size_m2 - room_size_m2, 0.0)

    result = fraction_from_area(
        residual_fire_area,
        residual_dwelling_area,
    )

    if result.fraction is not None and result.fraction > 0.0:
        return residual_fire_area, residual_dwelling_area, result, False

    # Same-band quartile fallback.
    #
    # Do not use this for every failed residual calculation. It is only valid
    # where the controlled model-facing band indices show a same-band ambiguity.
    (
        band_relationship,
        room_band_index,
        fire_band_index,
    ) = _multiple_rooms_area_band_relationship(event=event)

    if band_relationship != "same_band":
        return residual_fire_area, residual_dwelling_area, result, False

    origin_room_q25 = parse_area_band_position(
        event.get("building_room_origin_size_input"),
        position=0.25,
    )

    fire_area_q75 = parse_area_band_position(
        event.get("building_fire_damage_area_input"),
        position=0.75,
    )

    if origin_room_q25 is None or fire_area_q75 is None:
        return residual_fire_area, residual_dwelling_area, result, False

    fallback_residual_fire_area = fire_area_q75 - origin_room_q25

    if fallback_residual_fire_area <= 0.0:
        return residual_fire_area, residual_dwelling_area, result, False

    if dwelling_size_m2 is None:
        return residual_fire_area, residual_dwelling_area, result, False

    fallback_residual_dwelling_area = max(
        dwelling_size_m2 - origin_room_q25,
        0.0,
    )

    fallback_result = fraction_from_area(
        fallback_residual_fire_area,
        fallback_residual_dwelling_area,
    )

    if fallback_result.fraction is None or fallback_result.fraction <= 0.0:
        return residual_fire_area, residual_dwelling_area, result, False

    return (
        fallback_residual_fire_area,
        fallback_residual_dwelling_area,
        fallback_result,
        True,
    )


def _resolve_multiple_rooms_dwelling_damage_fraction(
    *,
    event: dict[str, Any],
    total_area_m2: Optional[float],
    fire_area_m2: Optional[float],
    dwelling_size_m2: Optional[float],
) -> tuple[Optional[float], FractionResult, bool, bool, str]:
    """
    Resolve dwelling damage fraction for multiple_rooms replacement.

    Ordinary calculation:
        building_total_damage_area_for_model / dwelling_size_m2

    Quartile fallback:
        If ordinary calculation fails or gives no positive damage fraction, use
        q75 of total damage area where available. If that is unavailable or zero,
        use q75 of fire damage area.

    Returns
    -------
    tuple
        damage_area_m2,
        fraction result,
        capped flag,
        fallback_used flag,
        area_basis label
    """
    total_area_capped, was_capped = capped_area(total_area_m2, dwelling_size_m2)
    result = fraction_from_area(total_area_capped, dwelling_size_m2)

    if result.fraction is not None and result.fraction > 0.0:
        return (
            total_area_capped,
            result,
            was_capped,
            False,
            "building_total_damage_area_for_model / dwelling_size_m2",
        )

    # Prefer q75 of total damage area.
    fallback_area = parse_area_band_position(
        event.get("building_total_damage_area_for_model")
        or event.get("building_total_damage_area_input"),
        position=0.75,
    )
    fallback_source = "q75(building_total_damage_area_for_model)"

    # If total damage is missing/zero, use q75 fire damage as fallback.
    if fallback_area is None or fallback_area <= 0.0:
        fallback_area = parse_area_band_position(
            event.get("building_fire_damage_area_input"),
            position=0.75,
        )
        fallback_source = "q75(building_fire_damage_area_input)"

    fallback_area_capped, fallback_was_capped = capped_area(
        fallback_area,
        dwelling_size_m2,
    )
    fallback_result = fraction_from_area(fallback_area_capped, dwelling_size_m2)

    if fallback_result.fraction is None or fallback_result.fraction <= 0.0:
        return (
            total_area_capped,
            result,
            was_capped,
            False,
            "building_total_damage_area_for_model / dwelling_size_m2",
        )

    return (
        fallback_area_capped,
        fallback_result,
        fallback_was_capped,
        True,
        f"{fallback_source} / dwelling_size_m2",
    )


def _make_stage1_row(
    *,
    event: dict[str, Any],
    inventory_snapshot_id: int,
    estimate_case: str,
    component_type: str,
    emission_pathway: str,
    area_basis: Optional[str],
    stock_basis: Optional[str],
    embodied_basis: Optional[str],
    direct_total_kgC: Optional[float],
    direct_biogenic_kgC: Optional[float],
    direct_fossil_kgC: Optional[float],
    replacement_embodied_CO2_kg: Optional[float],
    calculation_status: str,
    calculation_notes: Optional[str],
    created_at_utc: str,
    building_fire_damage_area_m2: Optional[float] = None,
    building_total_damage_area_m2: Optional[float] = None,
    room_of_origin_size_m2: Optional[float] = None,
    dwelling_size_m2: Optional[float] = None,
    residual_fire_area_m2: Optional[float] = None,
    residual_dwelling_area_m2: Optional[float] = None,
    replacement_damage_area_m2: Optional[float] = None,
    area_fraction: Optional[float] = None,
    room_fire_fraction: Optional[float] = None,
    room_damage_fraction: Optional[float] = None,
    residual_fire_fraction: Optional[float] = None,
    dwelling_damage_fraction: Optional[float] = None,
    affected_dwelling_count: int = 1,
) -> Stage1ComponentResult:
    """
    Create a Stage1ComponentResult while copying common event metadata.
    """
    return Stage1ComponentResult(
        source_id=event.get("source_id"),
        incident_id=event.get("incident_id"),
        input_type=event.get("input_type"),
        inventory_snapshot_id=inventory_snapshot_id,
        estimate_case=estimate_case,
        fiscal_year_start=event.get("fiscal_year_start"),
        fiscal_year_end=event.get("fiscal_year_end"),
        property_type_3_input=event.get("property_type_3_input"),
        dwelling_type=event.get("dwelling_type"),
        dwelling_type_proxy=event.get("dwelling_type_proxy"),
        dwelling_type_for_model=event.get("dwelling_type_for_model"),
        occupancy=event.get("occupancy"),
        fire_spread_category=normalise_fire_spread_category(event.get("fire_spread_category")),
        fire_spread_category_from_extent=event.get("fire_spread_category_from_extent"),
        room_of_origin=event.get("room_of_origin"),
        room_of_origin_proxy=event.get("room_of_origin_proxy"),
        single_item_status=event.get("single_item_status"),
        item_combusted=event.get("item_combusted"),
        component_type=component_type,
        emission_pathway=emission_pathway,
        area_basis=area_basis,
        stock_basis=stock_basis,
        embodied_basis=embodied_basis,
        building_fire_damage_area_input=event.get("building_fire_damage_area_input"),
        building_fire_damage_area_band_index=event.get("building_fire_damage_area_band_index"),
        building_fire_damage_area_m2=building_fire_damage_area_m2,
        building_total_damage_area_input=event.get("building_total_damage_area_input"),
        building_total_damage_area_for_model=event.get("building_total_damage_area_for_model") or event.get("building_total_damage_area_input"),
        building_total_damage_area_band_index=event.get("building_total_damage_area_band_index"),
        building_total_damage_area_m2=building_total_damage_area_m2,
        room_of_origin_size_m2=room_of_origin_size_m2,
        dwelling_size_m2=dwelling_size_m2,
        residual_fire_area_m2=residual_fire_area_m2,
        residual_dwelling_area_m2=residual_dwelling_area_m2,
        replacement_damage_area_m2=replacement_damage_area_m2,
        area_fraction=area_fraction,
        room_fire_fraction=room_fire_fraction,
        room_damage_fraction=room_damage_fraction,
        residual_fire_fraction=residual_fire_fraction,
        dwelling_damage_fraction=dwelling_damage_fraction,
        affected_dwelling_count=affected_dwelling_count,
        direct_total_kgC=direct_total_kgC,
        direct_biogenic_kgC=direct_biogenic_kgC,
        direct_fossil_kgC=direct_fossil_kgC,
        replacement_embodied_CO2_kg=replacement_embodied_CO2_kg,
        calculation_status=calculation_status,
        calculation_notes=calculation_notes,
        created_at_utc=created_at_utc,
    )


def _warning(
    *,
    event: dict[str, Any],
    created_at_utc: str,
    stage: str,
    warning_type: str,
    warning_text: str,
    fire_parameter: Optional[str] = None,
    raw_value: Optional[object] = None,
    resolved_value: Optional[object] = None,
    warning_severity: str = "warning",
    warning_category: Optional[str] = "fire_model_stage1",
) -> FireModelWarning:
    """
    Create one fire_model_warnings row.
    """
    return FireModelWarning(
        source_id=event.get("source_id"),
        incident_id=event.get("incident_id"),
        input_type=event.get("input_type"),
        stage=stage,
        warning_type=warning_type,
        warning_severity=warning_severity,
        warning_category=warning_category,
        warning_text=warning_text,
        fire_parameter=fire_parameter,
        raw_value=None if raw_value is None else str(raw_value),
        resolved_value=None if resolved_value is None else str(resolved_value),
        created_at_utc=created_at_utc,
    )
