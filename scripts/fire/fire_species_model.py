from __future__ import annotations

"""
Stage 2 species-emissions model for the Fire Emissions project.

Stage 1 answers:
    "How much carbon stock was directly affected by the fire?"

Stage 2 answers:
    "How much of that affected carbon is emitted as each combustion species?"

This module deliberately does NOT aggregate results.

It writes one long-format species row per:
    Stage 1 direct component
    x estimate_case
    x emitted species

For the first Stage 2 implementation, the emitted species are:
    - CO2
    - CO

Replacement embodied CO2 is not processed here.  It is already calculated in
Stage 1 and should remain a separate reporting stream.
"""

from math import exp
from typing import Any, Optional

from scripts.fire.emission_conversion_factors import carbon_to_species_mass_factor
from scripts.fire.fire_model_io import get_parameter_value
from scripts.fire.fire_model_records import FireModelWarning, Stage2SpeciesResult


# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------

# Keep estimate cases consistent with Stage 1.
ESTIMATE_CASES = {"low", "default", "high"}

# First-pass emitted species.
#
# The emission parameter workbook already expects these species-factor rows:
#   CO2_emission_factor_overventilated
#   CO2_emission_factor_underventilated
#   CO_emission_factor_overventilated
#   CO_emission_factor_underventilated
#
# The parameter ingester is deliberately generic and can later accept additional
# species rows with the same naming pattern.  For now, the model only writes
# CO2 and CO.
EMISSION_SPECIES = ["CO2", "CO"]

# Parameter names used by Stage 2.
PARAM_COMBUSTION_COMPLETENESS = "combustion_completeness_factor"
PARAM_CHAR_FORMATION = "char_formation_factor"
PARAM_FLASHOVER_ROOM_FRACTION = "flashover_room_fraction"
PARAM_FLAMEOVER_TRANSITION_WIDTH = "flameover_transition_width"
PARAM_COMPLETE_COMBUSTION_FLASHOVER_POSITION = "complete_combustion_flashover_position"

VENT_OVER = "overventilated"
VENT_UNDER = "underventilated"


# -----------------------------------------------------------------------------
# PUBLIC STAGE 2 ENTRY POINT
# -----------------------------------------------------------------------------

def build_stage2_species_results(
    *,
    stage1_direct_rows: list[dict[str, Any]],
    emission_parameters: dict[str, dict[str, dict[str, Optional[float]]]],
    created_at_utc: str,
) -> tuple[list[Stage2SpeciesResult], list[FireModelWarning]]:
    """
    Convert Stage 1 direct-carbon rows into Stage 2 species-emissions rows.

    Parameters
    ----------
    stage1_direct_rows:
        Rows from fire_model_stage1_component_results that represent direct
        affected carbon stock.

        The calling code should normally pre-filter to:
            emission_pathway = 'direct'
            direct_total_kgC > 0
            calculation_status = 'ok'

    emission_parameters:
        Nested dictionary returned by fire_model_io.load_emission_parameters().

        Shape:
            params[fire_spread_category][parameter_name][estimate_case]

    created_at_utc:
        Timestamp copied into output rows and warnings.

    Returns
    -------
    tuple[list[Stage2SpeciesResult], list[FireModelWarning]]
        Stage 2 species rows and any non-blocking model warnings.
    """
    results: list[Stage2SpeciesResult] = []
    warnings: list[FireModelWarning] = []

    for row in stage1_direct_rows:
        # Each Stage 1 direct row already belongs to one estimate case.
        # For example:
        #   incident A / origin_room / default
        estimate_case = str(row.get("estimate_case") or "").strip().lower()

        if estimate_case not in ESTIMATE_CASES:
            warnings.append(_warning(
                row=row,
                created_at_utc=created_at_utc,
                warning_type="STAGE2_UNKNOWN_ESTIMATE_CASE",
                warning_text=(
                    "Stage 1 row has an unknown estimate_case and was skipped "
                    "by Stage 2."
                ),
                raw_value=row.get("estimate_case"),
            ))
            continue

        fire_spread_category = _normalise_category(row.get("fire_spread_category"))

        # Heat/smoke-only rows should normally have zero direct carbon and should
        # already be absent from stage1_direct_rows.  This guard prevents accidental
        # species rows if an older table contains a non-zero heat/smoke direct row.
        if fire_spread_category == "heat_smoke_damage_only":
            warnings.append(_warning(
                row=row,
                created_at_utc=created_at_utc,
                warning_type="STAGE2_HEAT_SMOKE_DIRECT_ROW_SKIPPED",
                warning_text=(
                    "Stage 2 skipped a heat_smoke_damage_only direct row. "
                    "The current model treats heat/smoke-only incidents as zero "
                    "combustion emissions."
                ),
                raw_value=fire_spread_category,
            ))
            continue

        # Stage 2 should not convert replacement embodied CO2 into fire species.
        # The caller should already have filtered to direct rows, but this guard
        # makes the function safer if it is reused directly.
        if str(row.get("emission_pathway") or "").strip().lower() != "direct":
            continue

        direct_total_kgC = _to_float(row.get("direct_total_kgC"))
        direct_biogenic_kgC = _to_float(row.get("direct_biogenic_kgC"))
        direct_fossil_kgC = _to_float(row.get("direct_fossil_kgC"))

        if direct_total_kgC is None or direct_total_kgC <= 0.0:
            continue

        # If biogenic/fossil split is missing, keep total but leave the split
        # values blank.  This is better than guessing a split at Stage 2.
        # The current inventory path should normally provide all three values.
        combustion_completeness = _parameter_fraction(
            emission_parameters,
            fire_spread_category=fire_spread_category,
            parameter_name=PARAM_COMBUSTION_COMPLETENESS,
            estimate_case=estimate_case,
            default=1.0,
        )

        char_formation = _parameter_fraction(
            emission_parameters,
            fire_spread_category=fire_spread_category,
            parameter_name=PARAM_CHAR_FORMATION,
            estimate_case=estimate_case,
            default=0.0,
        )

        # Carbon available to become gas-phase species.
        #
        # Interpretation:
        #   direct carbon stock
        #   x combustion completeness
        #   x char / barrier adjustment factor
        #
        # In the current model, char_formation_factor is a placeholder multiplier.
        # A value of 1.0 leaves the combusted carbon unchanged. Future versions may use
        # lower values to represent inhibition by a char barrier layer.
        carbon_to_gas_factor = combustion_completeness * char_formation

        emitted_total_kgC = direct_total_kgC * carbon_to_gas_factor
        emitted_biogenic_kgC = _multiply_optional(direct_biogenic_kgC, carbon_to_gas_factor)
        emitted_fossil_kgC = _multiply_optional(direct_fossil_kgC, carbon_to_gas_factor)

        post_flashover_weighting = _resolve_post_flashover_weighting(
            row=row,
            emission_parameters=emission_parameters,
            fire_spread_category=fire_spread_category,
            estimate_case=estimate_case,
        )

        # Build one row per species.
        for species in EMISSION_SPECIES:
            species_rows, species_warnings = _build_species_row(
                row=row,
                fire_spread_category=fire_spread_category,
                estimate_case=estimate_case,
                species=species,
                emission_parameters=emission_parameters,
                combustion_completeness=combustion_completeness,
                char_formation=char_formation,
                emitted_total_kgC=emitted_total_kgC,
                emitted_biogenic_kgC=emitted_biogenic_kgC,
                emitted_fossil_kgC=emitted_fossil_kgC,
                post_flashover_weighting=post_flashover_weighting,
                created_at_utc=created_at_utc,
            )

            results.extend(species_rows)
            warnings.extend(species_warnings)

    return results, warnings


# -----------------------------------------------------------------------------
# SPECIES ROW BUILDING
# -----------------------------------------------------------------------------

def _build_species_row(
    *,
    row: dict[str, Any],
    fire_spread_category: str,
    estimate_case: str,
    species: str,
    emission_parameters: dict[str, dict[str, dict[str, Optional[float]]]],
    combustion_completeness: float,
    char_formation: float,
    emitted_total_kgC: float,
    emitted_biogenic_kgC: Optional[float],
    emitted_fossil_kgC: Optional[float],
    post_flashover_weighting: float,
    created_at_utc: str,
) -> tuple[list[Stage2SpeciesResult], list[FireModelWarning]]:
    """
    Build Stage 2 output rows for one emitted species.

    The Stage 2 table is long-format, so this function creates separate rows
    for each carbon origin:

        total
        biogenic
        fossil

    For each row:

        direct_affected_kgC
            = Stage 1 affected carbon for that carbon origin

        combusted_kgC
            = direct_affected_kgC
              * combustion_completeness_factor
              * char_formation_factor

        emitted_kg
            = combusted_kgC
              * species_emission_factor
              * molecular_conversion_factor

    The species_emission_factor is a weighted blend of overventilated and
    underventilated species factors.
    """
    warnings: list[FireModelWarning] = []
    results: list[Stage2SpeciesResult] = []

    over_param = f"{species}_emission_factor_{VENT_OVER}"
    under_param = f"{species}_emission_factor_{VENT_UNDER}"

    over_factor = _parameter_fraction(
        emission_parameters,
        fire_spread_category=fire_spread_category,
        parameter_name=over_param,
        estimate_case=estimate_case,
        default=None,
    )

    under_factor = _parameter_fraction(
        emission_parameters,
        fire_spread_category=fire_spread_category,
        parameter_name=under_param,
        estimate_case=estimate_case,
        default=None,
    )

    ventilation_condition_case = _ventilation_condition_case(post_flashover_weighting)

    if ventilation_condition_case == "overventilated":

        if over_factor is None:
            warnings.append(_warning(
                row=row,
                created_at_utc=created_at_utc,
                warning_type="STAGE2_SPECIES_FACTOR_MISSING",
                warning_text=(
                    f"Stage 2 could not calculate {species} because the "
                    "required overventilated species emission factor was missing."
                ),
                raw_value=over_param,
            ))
            return [], warnings

        species_emission_factor = over_factor
        species_factor_parameter_name = over_param

    elif ventilation_condition_case == "underventilated":

        if under_factor is None:
            warnings.append(_warning(
                row=row,
                created_at_utc=created_at_utc,
                warning_type="STAGE2_SPECIES_FACTOR_MISSING",
                warning_text=(
                    f"Stage 2 could not calculate {species} because the "
                    "required underventilated species emission factor was missing."
                ),
                raw_value=under_param,
            ))
            return [], warnings

        species_emission_factor = under_factor
        species_factor_parameter_name = under_param

    else:

        if over_factor is None or under_factor is None:
            warnings.append(_warning(
                row=row,
                created_at_utc=created_at_utc,
                warning_type="STAGE2_SPECIES_FACTOR_MISSING",
                warning_text=(
                    f"Stage 2 could not calculate {species} because one or more "
                    "required blended species emission factors were missing."
                ),
                raw_value=f"{over_param}; {under_param}",
            ))
            return [], warnings

        species_emission_factor = (
            (1.0 - post_flashover_weighting) * over_factor
            + post_flashover_weighting * under_factor
        )

        species_factor_parameter_name = f"blended:{over_param};{under_param}"

    molecular_conversion_factor = carbon_to_species_mass_factor(species)

    carbon_partition_sum = _carbon_partition_sum(
        emission_parameters=emission_parameters,
        fire_spread_category=fire_spread_category,
        estimate_case=estimate_case,
        post_flashover_weighting=post_flashover_weighting,
    )

    fire_development_case = _fire_development_case(
        row=row,
        fire_spread_category=fire_spread_category,
    )

    carbon_origin_values = {
        "total": (
            _to_float(row.get("direct_total_kgC")),
            emitted_total_kgC,
        ),
        "biogenic": (
            _to_float(row.get("direct_biogenic_kgC")),
            emitted_biogenic_kgC,
        ),
        "fossil": (
            _to_float(row.get("direct_fossil_kgC")),
            emitted_fossil_kgC,
        ),
    }

    for carbon_origin, (direct_affected_kgC, combusted_kgC) in carbon_origin_values.items():

        if direct_affected_kgC is None or combusted_kgC is None:
            continue

        emitted_kg = (
            combusted_kgC
            * species_emission_factor
            * molecular_conversion_factor
        )

        results.append(Stage2SpeciesResult(
            stage1_result_id=_to_int(row.get("stage1_result_id")),
            source_id=row.get("source_id"),
            incident_id=row.get("incident_id"),
            input_type=row.get("input_type"),
            inventory_snapshot_id=_to_int(row.get("inventory_snapshot_id")),

            estimate_case=estimate_case,

            fiscal_year_start=_to_int(row.get("fiscal_year_start")),
            fiscal_year_end=_to_int(row.get("fiscal_year_end")),
            property_type_3_input=row.get("property_type_3_input"),
            dwelling_type=row.get("dwelling_type"),
            dwelling_type_proxy=row.get("dwelling_type_proxy"),
            dwelling_type_for_model=row.get("dwelling_type_for_model"),
            occupancy=row.get("occupancy"),

            fire_spread_category=fire_spread_category,
            fire_spread_category_from_extent=row.get("fire_spread_category_from_extent"),

            room_of_origin=row.get("room_of_origin"),
            room_of_origin_proxy=row.get("room_of_origin_proxy"),

            component_type=str(row.get("component_type") or ""),

            emission_species=species,
            carbon_origin=carbon_origin,

            parameter_fire_spread_category=fire_spread_category,
            species_factor_parameter_name=species_factor_parameter_name,
            ventilation_condition_case=ventilation_condition_case,
            fire_development_case=fire_development_case,

            direct_affected_kgC=direct_affected_kgC,
            combusted_kgC=combusted_kgC,

            combustion_completeness_factor=combustion_completeness,
            char_formation_factor=char_formation,
            post_flashover_weighting=post_flashover_weighting,

            species_emission_factor=species_emission_factor,
            molecular_conversion_factor=molecular_conversion_factor,

            emitted_kg=emitted_kg,

            carbon_partition_sum=carbon_partition_sum,

            calculation_status="ok",
            calculation_notes=(
                "Stage 2 species row derived from Stage 1 direct carbon stock. "
                "Replacement embodied CO2 is excluded from species conversion."
            ),
            created_at_utc=created_at_utc,
        ))

    return results, warnings


def _carbon_partition_sum(
    *,
    emission_parameters: dict[str, dict[str, dict[str, Optional[float]]]],
    fire_spread_category: str,
    estimate_case: str,
    post_flashover_weighting: float,
) -> Optional[float]:
    """
    Sum the carbon-allocation factors across the currently modelled species.

    The required parameters depend on the ventilation case:

        overventilated:
            require only <species>_emission_factor_overventilated

        underventilated:
            require only <species>_emission_factor_underventilated

        blended:
            require both overventilated and underventilated factors

    This allows single_item cases to use only overventilated species factors.
    """
    total = 0.0
    ventilation_condition_case = _ventilation_condition_case(post_flashover_weighting)

    for species in EMISSION_SPECIES:
        over_param = f"{species}_emission_factor_{VENT_OVER}"
        under_param = f"{species}_emission_factor_{VENT_UNDER}"

        over_factor = _parameter_fraction(
            emission_parameters,
            fire_spread_category=fire_spread_category,
            parameter_name=over_param,
            estimate_case=estimate_case,
            default=None,
        )

        under_factor = _parameter_fraction(
            emission_parameters,
            fire_spread_category=fire_spread_category,
            parameter_name=under_param,
            estimate_case=estimate_case,
            default=None,
        )

        if ventilation_condition_case == "overventilated":

            if over_factor is None:
                return None

            mixed_factor = over_factor

        elif ventilation_condition_case == "underventilated":

            if under_factor is None:
                return None

            mixed_factor = under_factor

        else:

            if over_factor is None or under_factor is None:
                return None

            mixed_factor = (
                (1.0 - post_flashover_weighting) * over_factor
                + post_flashover_weighting * under_factor
            )

        total += mixed_factor

    return total

def _ventilation_condition_case(post_flashover_weighting: float) -> str:
    """
    Label the ventilation case used for the species factor.

    Must match the CHECK constraint in fire_model_stage2_species_results:

        overventilated
        underventilated
        blended
        not_applicable
    """
    if post_flashover_weighting <= 0.0:
        return "overventilated"

    if post_flashover_weighting >= 1.0:
        return "underventilated"

    return "blended"


def _fire_development_case(
    *,
    row: dict[str, Any],
    fire_spread_category: str,
) -> str:
    """
    Label the fire-development proxy used for the post-flashover weighting.
    """
    component_type = str(row.get("component_type") or "").strip().lower()

    if fire_spread_category == "single_item":
        return "single_item_overventilated"

    if fire_spread_category == "within_room":
        return "partial_room_sigmoid"

    if fire_spread_category == "multiple_rooms":
        if component_type == "origin_room":
            return "complete_component_integrated_sigmoid"

        if component_type == "residual_dwelling":
            return "residual_dwelling_sigmoid"

    if fire_spread_category == "entire_dwelling":
        return "complete_component_integrated_sigmoid"

    return "unknown"


# -----------------------------------------------------------------------------
# FLASHOVER / VENTILATION WEIGHTING
# -----------------------------------------------------------------------------

def _resolve_post_flashover_weighting(
    *,
    row: dict[str, Any],
    emission_parameters: dict[str, dict[str, dict[str, Optional[float]]]],
    fire_spread_category: str,
    estimate_case: str,
) -> float:
    """
    Estimate the post-flashover / underventilated weighting for one Stage 1
    direct-carbon component.

    Returned value:
        0 = use overventilated species factors
        1 = use underventilated species factors

    The logic differs by component type:

        single_item:
            Treated as overventilated in this first-pass model.

        within_room:
            Uses room_fire_fraction and the parameter flashover_room_fraction.

        multiple_rooms / origin_room:
            Uses an integrated sigmoid with centre set by
            complete_combustion_flashover_position.

        multiple_rooms / residual_dwelling:
            Uses residual_fire_fraction and the parameter flashover_room_fraction.

        entire_dwelling:
            Uses an integrated sigmoid with centre set by
            complete_combustion_flashover_position.

    The reason for using complete_combustion_flashover_position for complete
    room/dwelling components is that these rows represent complete involvement,
    but we do not want to assume all carbon burned under the final fully
    underventilated state.
    """

    # Partial-room flashover position.
    flashover_room_fraction = _parameter_fraction(
        emission_parameters,
        fire_spread_category=fire_spread_category,
        parameter_name=PARAM_FLASHOVER_ROOM_FRACTION,
        estimate_case=estimate_case,
        default=0.6,
    )

    # Sigmoid transition width.
    transition_width = _parameter_positive(
        emission_parameters,
        fire_spread_category=fire_spread_category,
        parameter_name=PARAM_FLAMEOVER_TRANSITION_WIDTH,
        estimate_case=estimate_case,
        default=0.1,
    )

    # Complete-combustion flashover position.
    complete_combustion_flashover_position = _parameter_fraction(
        emission_parameters,
        fire_spread_category=fire_spread_category,
        parameter_name=PARAM_COMPLETE_COMBUSTION_FLASHOVER_POSITION,
        estimate_case=estimate_case,
        default=flashover_room_fraction,
    )

    component_type = str(row.get("component_type") or "").strip().lower()

    # Single-item fires are treated as overventilated in this first pass.
    if fire_spread_category == "single_item":
        return 0.0

    # Partial within-room fire: use the resolved room fire fraction.
    if fire_spread_category == "within_room":
        room_fraction = _to_float(row.get("room_fire_fraction"))

        if room_fraction is None:
            room_fraction = _to_float(row.get("area_fraction"))  # fallback

        if room_fraction is None:
            return 0.0

        return _sigmoid_weight(
            x=_clamp_fraction(room_fraction),
            transition_end=flashover_room_fraction,
            transition_width=transition_width,
        )

    # Multiple-room fire: origin room is treated as complete room involvement.
    if fire_spread_category == "multiple_rooms":
        if component_type == "origin_room":
            return _integrated_sigmoid_weight(
                transition_end=complete_combustion_flashover_position,
                transition_width=transition_width,
            )

        if component_type == "residual_dwelling":
            residual_fraction = _to_float(row.get("residual_fire_fraction"))

            if residual_fraction is None:
                residual_fraction = _to_float(row.get("area_fraction"))  # fallback

            if residual_fraction is None:
                return 0.0

            return _sigmoid_weight(
                x=_clamp_fraction(residual_fraction),
                transition_end=flashover_room_fraction,
                transition_width=transition_width,
            )

        return 0.0

    # Entire-dwelling fire: use complete-combustion integrated weighting.
    if fire_spread_category == "entire_dwelling":
        return _integrated_sigmoid_weight(
            transition_end=complete_combustion_flashover_position,
            transition_width=transition_width,
        )

    return 0.0


def _sigmoid_weight(*, x: float, transition_end: float, transition_width: float) -> float:
    """
    Smooth transition from overventilated to underventilated weighting.

    Interpretation:
        transition_end is the fire-development point where the transition is
        treated as essentially complete. In the current model, this is the
        flashover-position parameter.

        transition_width is the approximate width of the pre-flashover
        flameover / rollover transition zone.

    Returned value:
        0 = overventilated weighting
        1 = underventilated weighting
    """
    x = _clamp_fraction(x)
    transition_end = _clamp_fraction(transition_end)

    if transition_width <= 0:
        return 1.0 if x >= transition_end else 0.0

    centre = transition_end - (transition_width / 2.0)

    z = max(-60.0, min(60.0, (x - centre) / transition_width))

    return _clamp_fraction(1.0 / (1.0 + exp(-z)))


def _integrated_sigmoid_weight(
    *,
    transition_end: float,
    transition_width: float,
    n_points: int = 101,
) -> float:
    """
    Average the sigmoid across complete combustion progress from 0 to 1.

    Why this exists
    ---------------
    For a whole room or whole dwelling component, using x = 1 directly would
    imply that the entire component burns under the final underventilated state.

    That is probably too strong.  Instead, this helper averages the sigmoid from
    progress 0 to progress 1.  This approximates the idea that the component
    passes through a combustion-development pathway. I.e. starts off as overventillatied
    and progresses through flashover to underventillated.
    """
    if n_points < 2:
        raise ValueError("n_points must be at least 2.")

    values = []

    for i in range(n_points):
        x = i / (n_points - 1)
        values.append(
            _sigmoid_weight(
                x=x,
                transition_end=transition_end,
                transition_width=transition_width,
            )
        )

    return sum(values) / len(values)

# -----------------------------------------------------------------------------
# PARAMETER HELPERS
# -----------------------------------------------------------------------------

def _parameter_fraction(
    params: dict[str, dict[str, dict[str, Optional[float]]]],
    *,
    fire_spread_category: str,
    parameter_name: str,
    estimate_case: str,
    default: Optional[float],
) -> Optional[float]:
    """
    Read a parameter that should be a fraction in [0, 1].

    If the parameter is absent and a default is supplied, the default is used.
    If the parameter is absent and default is None, None is returned.

    A present value outside [0, 1] raises a ValueError because that would make
    the Stage 2 calculation physically ambiguous.
    """
    value = get_parameter_value(
        params,
        fire_spread_category=fire_spread_category,
        parameter_name=parameter_name,
        estimate_case=estimate_case,
        default=default,
    )

    if value is None:
        return None

    value = float(value)

    if value < 0.0 or value > 1.0:
        raise ValueError(
            f"Parameter {parameter_name!r} for category "
            f"{fire_spread_category!r} / estimate_case {estimate_case!r} "
            f"must be between 0 and 1. Found: {value}."
        )

    return value


def _parameter_positive(
    params: dict[str, dict[str, dict[str, Optional[float]]]],
    *,
    fire_spread_category: str,
    parameter_name: str,
    estimate_case: str,
    default: float,
) -> float:
    """
    Read a parameter that should be positive.

    Used for transition widths.  Zero or negative values would make the smooth
    sigmoid undefined, so they are rejected.
    """
    value = get_parameter_value(
        params,
        fire_spread_category=fire_spread_category,
        parameter_name=parameter_name,
        estimate_case=estimate_case,
        default=default,
    )

    if value is None:
        value = default

    value = float(value)

    if value <= 0.0:
        raise ValueError(
            f"Parameter {parameter_name!r} for category "
            f"{fire_spread_category!r} / estimate_case {estimate_case!r} "
            f"must be positive. Found: {value}."
        )

    return value


# -----------------------------------------------------------------------------
# SMALL VALUE HELPERS
# -----------------------------------------------------------------------------

def _normalise_category(value: object) -> str:
    """
    Normalise fire-spread category labels used by older and newer drafts.
    """
    if value is None:
        return "unspecified"

    text = str(value).strip().lower()

    if text == "heat_smoke":
        return "heat_smoke_damage_only"

    return text


def _to_float(value: object) -> Optional[float]:
    """
    Convert a database value to float, preserving None / blank as None.
    """
    if value is None:
        return None

    text = str(value).strip()

    if text == "":
        return None

    return float(value)


def _to_int(value: object) -> Optional[int]:
    """
    Convert a database value to int, preserving None / blank as None.
    """
    if value is None:
        return None

    text = str(value).strip()

    if text == "":
        return None

    return int(float(text))


def _multiply_optional(value: Optional[float], factor: float) -> Optional[float]:
    """
    Multiply a possibly-missing value by a factor.
    """
    if value is None:
        return None

    return value * factor


def _clamp_fraction(value: float) -> float:
    """
    Clamp a number to the interval [0, 1].
    """
    return max(0.0, min(1.0, float(value)))


def _warning(
    *,
    row: dict[str, Any],
    created_at_utc: str,
    warning_type: str,
    warning_text: str,
    raw_value: object = None,
    resolved_value: object = None,
) -> FireModelWarning:
    """
    Create one structured model warning linked to the Stage 1 row's incident.
    """
    return FireModelWarning(
        source_id=row.get("source_id"),
        incident_id=row.get("incident_id"),
        input_type=row.get("input_type"),
        stage="stage_2_species",
        warning_type=warning_type,
        warning_severity="warning",
        warning_category="species_conversion",
        warning_text=warning_text,
        fire_parameter=None,
        raw_value=None if raw_value is None else str(raw_value),
        resolved_value=None if resolved_value is None else str(resolved_value),
        created_at_utc=created_at_utc,
    )