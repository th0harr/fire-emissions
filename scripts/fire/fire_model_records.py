from __future__ import annotations

"""
Shared record objects for the deterministic Fire Emissions model.

This module deliberately contains no SQLite code and no calculation logic.
It only defines small containers that can be passed between the database I/O,
area, and stock-modelling modules.

Why use dataclasses here?
-------------------------
The Stage 1 model has many fields, and passing large anonymous dictionaries
between functions becomes difficult to debug very quickly.  Dataclasses let us
keep the same simple Python structure, but with named fields and a clear
``to_insert_dict()`` method for database writing.
"""

from dataclasses import asdict, dataclass
from typing import Any, Optional


# -----------------------------------------------------------------------------
# GENERIC HELPERS
# -----------------------------------------------------------------------------

def _round_small_negative(value: Optional[float]) -> Optional[float]:
    """
    Clean very small negative values caused by floating point arithmetic.

    This is not intended to hide real negative model outputs.  It only prevents
    values such as -1e-15 from being inserted into columns with >= 0 checks.
    """
    if value is None:
        return None

    if -1e-12 < value < 0:
        return 0.0

    return value


# -----------------------------------------------------------------------------
# AREA ESTIMATE RECORD
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class AreaEstimate:
    """
    Low/default/high estimate for one FRIS area band.

    The fire-event resolver keeps the original area-band labels because FRIS
    records damage as ranges such as "6-10" rather than a measured area.
    Stage 1 needs numeric areas, so this record stores the three deterministic
    values that we use for the low/default/high estimate cases.
    """

    input_label: Optional[str]
    low_m2: Optional[float]
    default_m2: Optional[float]
    high_m2: Optional[float]
    is_none_band: bool = False
    is_open_ended: bool = False

    def get(self, estimate_case: str) -> Optional[float]:
        """
        Return the area value for one estimate case.
        """
        if estimate_case == "low":
            return self.low_m2
        if estimate_case == "default":
            return self.default_m2
        if estimate_case == "high":
            return self.high_m2
        raise ValueError(f"Unknown estimate_case: {estimate_case}")


# -----------------------------------------------------------------------------
# STOCK ESTIMATE RECORDS
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class CarbonStockEstimate:
    """
    Low/default/high carbon stock estimate.

    Values are stored separately for total, biogenic and fossil carbon because
    the later gas-species model needs to preserve this split.
    """

    total_low_kgC: Optional[float]
    total_default_kgC: Optional[float]
    total_high_kgC: Optional[float]

    biogenic_low_kgC: Optional[float]
    biogenic_default_kgC: Optional[float]
    biogenic_high_kgC: Optional[float]

    fossil_low_kgC: Optional[float]
    fossil_default_kgC: Optional[float]
    fossil_high_kgC: Optional[float]

    def total(self, estimate_case: str) -> Optional[float]:
        if estimate_case == "low":
            return self.total_low_kgC
        if estimate_case == "default":
            return self.total_default_kgC
        if estimate_case == "high":
            return self.total_high_kgC
        raise ValueError(f"Unknown estimate_case: {estimate_case}")

    def biogenic(self, estimate_case: str) -> Optional[float]:
        if estimate_case == "low":
            return self.biogenic_low_kgC
        if estimate_case == "default":
            return self.biogenic_default_kgC
        if estimate_case == "high":
            return self.biogenic_high_kgC
        raise ValueError(f"Unknown estimate_case: {estimate_case}")

    def fossil(self, estimate_case: str) -> Optional[float]:
        if estimate_case == "low":
            return self.fossil_low_kgC
        if estimate_case == "default":
            return self.fossil_default_kgC
        if estimate_case == "high":
            return self.fossil_high_kgC
        raise ValueError(f"Unknown estimate_case: {estimate_case}")

    def multiply(self, factor: float) -> "CarbonStockEstimate":
        """
        Scale all carbon values by the same fraction.
        """
        def m(value: Optional[float]) -> Optional[float]:
            if value is None:
                return None
            return _round_small_negative(value * factor)

        return CarbonStockEstimate(
            total_low_kgC=m(self.total_low_kgC),
            total_default_kgC=m(self.total_default_kgC),
            total_high_kgC=m(self.total_high_kgC),
            biogenic_low_kgC=m(self.biogenic_low_kgC),
            biogenic_default_kgC=m(self.biogenic_default_kgC),
            biogenic_high_kgC=m(self.biogenic_high_kgC),
            fossil_low_kgC=m(self.fossil_low_kgC),
            fossil_default_kgC=m(self.fossil_default_kgC),
            fossil_high_kgC=m(self.fossil_high_kgC),
        )

    def subtract(self, other: "CarbonStockEstimate") -> "CarbonStockEstimate":
        """
        Subtract another stock estimate, clipping tiny/negative residuals at 0.

        This is mainly used for residual dwelling stock:
            whole dwelling stock - origin room stock
        """
        def s(a: Optional[float], b: Optional[float]) -> Optional[float]:
            if a is None or b is None:
                return None
            return max(0.0, _round_small_negative(a - b) or 0.0)

        return CarbonStockEstimate(
            total_low_kgC=s(self.total_low_kgC, other.total_low_kgC),
            total_default_kgC=s(self.total_default_kgC, other.total_default_kgC),
            total_high_kgC=s(self.total_high_kgC, other.total_high_kgC),
            biogenic_low_kgC=s(self.biogenic_low_kgC, other.biogenic_low_kgC),
            biogenic_default_kgC=s(self.biogenic_default_kgC, other.biogenic_default_kgC),
            biogenic_high_kgC=s(self.biogenic_high_kgC, other.biogenic_high_kgC),
            fossil_low_kgC=s(self.fossil_low_kgC, other.fossil_low_kgC),
            fossil_default_kgC=s(self.fossil_default_kgC, other.fossil_default_kgC),
            fossil_high_kgC=s(self.fossil_high_kgC, other.fossil_high_kgC),
        )


@dataclass(frozen=True)
class EmbodiedCO2Estimate:
    """
    Low/default/high embodied CO2 estimate for replacement calculations.
    """

    low_kg: Optional[float]
    default_kg: Optional[float]
    high_kg: Optional[float]

    def get(self, estimate_case: str) -> Optional[float]:
        if estimate_case == "low":
            return self.low_kg
        if estimate_case == "default":
            return self.default_kg
        if estimate_case == "high":
            return self.high_kg
        raise ValueError(f"Unknown estimate_case: {estimate_case}")

    def multiply(self, factor: float) -> "EmbodiedCO2Estimate":
        """
        Scale all embodied CO2 values by the same replacement fraction.
        """
        def m(value: Optional[float]) -> Optional[float]:
            if value is None:
                return None
            return _round_small_negative(value * factor)

        return EmbodiedCO2Estimate(
            low_kg=m(self.low_kg),
            default_kg=m(self.default_kg),
            high_kg=m(self.high_kg),
        )

    def subtract(self, other: "EmbodiedCO2Estimate") -> "EmbodiedCO2Estimate":
        """
        Subtract another embodied CO2 estimate, clipping at 0.
        """
        def s(a: Optional[float], b: Optional[float]) -> Optional[float]:
            if a is None or b is None:
                return None
            return max(0.0, _round_small_negative(a - b) or 0.0)

        return EmbodiedCO2Estimate(
            low_kg=s(self.low_kg, other.low_kg),
            default_kg=s(self.default_kg, other.default_kg),
            high_kg=s(self.high_kg, other.high_kg),
        )


@dataclass(frozen=True)
class RoomStockRecord:
    """
    Carbon stock and embodied CO2 lookup for one room_type.
    """

    inventory_snapshot_id: int
    room_type: str
    room_description: Optional[str]
    room_size_m2: Optional[float]
    carbon: CarbonStockEstimate
    embodied_CO2: EmbodiedCO2Estimate


@dataclass(frozen=True)
class ItemStockRecord:
    """
    Carbon stock lookup for one inventory item.

    The current item lookup view only exposes one central estimate, so low and
    high item carbon values are initially the same as default.
    """

    inventory_snapshot_id: int
    item_name: str
    carbon: CarbonStockEstimate


# -----------------------------------------------------------------------------
# MODEL OUTPUT RECORDS
# -----------------------------------------------------------------------------

@dataclass
class Stage1ComponentResult:
    """
    One row for fire_model_stage1_component_results.

    This is the output of Stage 1: affected stock and replacement embodied CO2.
    The later Stage 2 species-emissions model should consume only rows where
    emission_pathway == "direct" and direct_total_kgC > 0.
    """

    source_id: Optional[str]
    incident_id: Optional[str]
    input_type: Optional[str]
    inventory_snapshot_id: Optional[int]

    estimate_case: str

    fiscal_year_start: Optional[int]
    fiscal_year_end: Optional[int]

    property_type_3_input: Optional[str]
    dwelling_type: Optional[str]
    dwelling_type_proxy: Optional[str]
    dwelling_type_for_model: Optional[str]
    occupancy: Optional[str]

    fire_spread_category: str
    fire_spread_category_from_extent: Optional[str]

    room_of_origin: Optional[str]
    room_of_origin_proxy: Optional[str]

    single_item_status: Optional[str]
    item_combusted: Optional[str]

    component_type: str
    emission_pathway: str

    area_basis: Optional[str]
    stock_basis: Optional[str]
    embodied_basis: Optional[str]

    building_fire_damage_area_input: Optional[str]
    building_fire_damage_area_band_index: Optional[int]
    building_fire_damage_area_m2: Optional[float]

    building_total_damage_area_input: Optional[str]
    building_total_damage_area_for_model: Optional[str]
    building_total_damage_area_band_index: Optional[int]
    building_total_damage_area_m2: Optional[float]

    room_of_origin_size_m2: Optional[float]
    dwelling_size_m2: Optional[float]

    residual_fire_area_m2: Optional[float]
    residual_dwelling_area_m2: Optional[float]
    replacement_damage_area_m2: Optional[float]

    area_fraction: Optional[float]
    room_fire_fraction: Optional[float]
    room_damage_fraction: Optional[float]
    residual_fire_fraction: Optional[float]
    dwelling_damage_fraction: Optional[float]
    affected_dwelling_count: int          # total dwellings represented by this row

    direct_total_kgC: Optional[float]
    direct_biogenic_kgC: Optional[float]
    direct_fossil_kgC: Optional[float]

    replacement_embodied_CO2_kg: Optional[float]

    calculation_status: str
    calculation_notes: Optional[str]

    created_at_utc: str

    def to_insert_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class Stage2SpeciesResult:
    """
    One row for fire_model_stage2_species_results.

    Stage 2 converts Stage 1 direct affected-carbon rows into emitted combustion
    species rows.

    Important:
        This table should only represent combustion-species emissions derived
        from direct carbon stock.

        It should not include replacement embodied CO2, because replacement
        embodied CO2 is not a fire-combustion species. Replacement emissions
        remain available in Stage 1 for later reporting.

    Row structure:
        The Stage 2 table is long-format.

        Each row represents one combination of:
            Stage 1 component
            x estimate_case
            x emission_species
            x carbon_origin

        For example:
            CO2 / total
            CO2 / biogenic
            CO2 / fossil
            CO  / total
            CO  / biogenic
            CO  / fossil
    """

    # Link / provenance fields.
    stage1_result_id: Optional[int]      # link back to Stage 1 row, if available

    source_id: Optional[str]
    incident_id: Optional[str]
    input_type: Optional[str]
    inventory_snapshot_id: Optional[int]

    # Scenario / uncertainty case.
    estimate_case: str                   # matches Stage 1 estimate_case

    # Event metadata copied forward from Stage 1.
    fiscal_year_start: Optional[int]
    fiscal_year_end: Optional[int]
    property_type_3_input: Optional[str]
    dwelling_type: Optional[str]
    dwelling_type_proxy: Optional[str]
    dwelling_type_for_model: Optional[str]
    occupancy: Optional[str]

    # Fire classification metadata.
    fire_spread_category: str
    fire_spread_category_from_extent: Optional[str]

    room_of_origin: Optional[str]
    room_of_origin_proxy: Optional[str]

    # Component metadata copied from Stage 1.
    component_type: str                  # e.g. single_item, origin_room, whole_dwelling

    # Species and carbon-origin metadata.
    emission_species: str                # e.g. CO2, CO
    carbon_origin: str                   # total, biogenic, or fossil

    # Parameter provenance.
    parameter_fire_spread_category: Optional[str]
    species_factor_parameter_name: Optional[str]
    ventilation_condition_case: Optional[str]
    fire_development_case: Optional[str]

    # Carbon-stock input and combustion transformation.
    direct_affected_kgC: Optional[float]
    combusted_kgC: Optional[float]             # after completeness and char multiplier

    combustion_completeness_factor: Optional[float]
    char_formation_factor: Optional[float]     # placeholder char/barrier multiplier
    post_flashover_weighting: Optional[float]

    species_emission_factor: Optional[float]
    molecular_conversion_factor: Optional[float]

    # Final emitted species mass.
    emitted_kg: Optional[float]

    # Carbon-partition check.
    carbon_partition_sum: Optional[float]      # e.g. CO2 factor + CO factor

    # Calculation status fields.
    calculation_status: str
    calculation_notes: Optional[str]

    created_at_utc: str

    def to_insert_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class FireModelWarning:
    """
    One row for fire_model_warnings.
    """

    source_id: Optional[str]
    incident_id: Optional[str]
    input_type: Optional[str]

    stage: str
    warning_type: str
    warning_severity: str
    warning_category: Optional[str]
    warning_text: str

    fire_parameter: Optional[str]
    raw_value: Optional[str]
    resolved_value: Optional[str]

    created_at_utc: str

    def to_insert_dict(self) -> dict[str, Any]:
        return asdict(self)
