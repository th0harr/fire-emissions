# scripts/fire/emission_parameters.py
from __future__ import annotations

import re
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

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

# Controlled workbook name.
# The dispatcher wrapper should look for this file inside the raw/config folder
# resolved from local_paths.yaml using --type emissions.
EXPECTED_FILENAME = "emission_param.xlsx"

# Controlled worksheet containing the parameter blocks.
SHEET_FIRE_CATEGORY_PARAMS = "fire_category_params"

# Destination table in fire_db.
TABLE_EMISSION_PARAMS = "fire_emission_parameter_mapping"

# Shared source/log tables already used in fire_db.
TABLE_SOURCES = "sources"
TABLE_INGEST_LOG = "ingest_log"

# Source metadata used when recording the workbook in the sources table.
SOURCE_TYPE = "emissions"
SOURCE_DESCRIPTION = "Fire emission parameter workbook"
SOURCE_ORG = "internal"


# -----------------------------------------------------------------------------
# Fire spread categories
# -----------------------------------------------------------------------------

# These are the combustion-relevant fire-spread categories that require
# emission parameters.
#
# Note:
#   heat_smoke is intentionally absent because it does not require combustion
#   parameters. The model should bypass this table for heat_smoke cases and use
#   smoke_heat_damage_area_m2 directly.
VALID_FIRE_SPREAD_CATEGORIES = {
    "single_item",
    "within_room",
    "multiple_rooms",
    "entire_dwelling",
}

REQUIRED_FIRE_SPREAD_CATEGORIES = {
    "single_item",
    "within_room",
    "multiple_rooms",
    "entire_dwelling",
}


# -----------------------------------------------------------------------------
# Parameter names
# -----------------------------------------------------------------------------

# Non-species model-control parameters currently expected in the workbook.
#
# Species emission factors are handled separately using a generic naming pattern:
#   {species}_emission_factor_{ventilation_condition}
#
# This allows us to add rows such as:
#   CH4_emission_factor_overventilated
# later, without having to rewrite the validation code.
KNOWN_MODEL_CONTROL_PARAMETERS = {
    "flashover_room_fraction",
    "flameover_transition_width",
    "complete_combustion_flashover_position",
    "combustion_completeness_factor",
    "char_formation_factor",
    "additional_replacement_factor",
}

# Current expected emission-factor rows.
# These are not the only possible emission-factor rows, but requiring these
# helps catch accidental deletion of the current CO2/CO rows.
CURRENT_REQUIRED_SPECIES_PARAMETERS = {
    "CO2_emission_factor_overventilated",
    "CO2_emission_factor_underventilated",
    "CO_emission_factor_overventilated",
    "CO_emission_factor_underventilated",
}

# All currently expected rows.
# Extra future species rows are allowed if they match the generic species pattern.
CURRENT_REQUIRED_PARAMETERS = (
    KNOWN_MODEL_CONTROL_PARAMETERS
    | CURRENT_REQUIRED_SPECIES_PARAMETERS
)

# Pattern for species emission factor rows.
#
# Examples:
#   CO2_emission_factor_overventilated
#   CO_emission_factor_underventilated
#   CH4_emission_factor_overventilated
#
# Species is deliberately flexible.
# Ventilation condition is validated separately.
SPECIES_EMISSION_FACTOR_RE = re.compile(
    r"^(?P<species>[A-Za-z0-9]+)_emission_factor_(?P<ventilation_condition>[A-Za-z0-9_]+)$"
)

VALID_VENTILATION_CONDITIONS = {
    "overventilated",
    "underventilated",
}

# Text placeholders that mean "not applicable" or blank.
NULL_LIKE_TEXT = {
    "",
    "none",
    "nan",
    "na",
    "n/a",
    "null",
}

# Text placeholders that explicitly mean a row is not applicable.
NA_LIKE_TEXT = {
    "na",
    "n/a",
}


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class EmissionParameterRow:
    """
    One tidy parameter row parsed from the fire emission parameter workbook.

    One row represents:
        fire_spread_category x fire_emission_parameter

    The workbook has separate blocks for each fire spread category, but the
    database stores them in one tidy mapping table.
    """

    fire_spread_category: str

    fire_emission_parameter: str
    parameter_type: str

    emission_species: Optional[str]
    ventilation_condition: Optional[str]

    is_applicable: int

    value_min: Optional[float]
    value_default: Optional[float]
    value_max: Optional[float]

    notes: Optional[str]

    source_sheet: str
    source_table: str
    input_row_number: int


@dataclass(frozen=True)
class EmissionParameterWorkbook:
    """
    Parsed workbook result.

    This intentionally mirrors the simple `vocab.py` style:
        - rows are the validated dataclass objects ready for insertion
        - warnings are non-blocking messages that should be shown to the user

    Blocking issues are raised as ValueError during parsing/validation.
    """

    rows: list[EmissionParameterRow]
    warnings: list[str]


# -----------------------------------------------------------------------------
# Public workbook reader
# -----------------------------------------------------------------------------

def read_emission_parameters_xlsx_pandas(
    xlsx_path: str | Path,
) -> EmissionParameterWorkbook:
    """
    Read and validate emission parameters from emission_param.xlsx.

    Expected workbook layout
    ------------------------
    Sheet:
        fire_category_params

    Each category block:
        row 1:  fire_spread_category, e.g. single_item_only
        row 2:  fire_emission_parameter | min | default | max | notes
        rows:   parameter values

    Important assumptions
    ---------------------
    - heat_smoke cases do not need emission-parameter rows.
    - The deterministic model will use value_default.
    - value_min and value_max are mainly for later sensitivity analysis.
    - N/A in min/default/max means the parameter is not applicable for that
      fire-spread category.
    - Molecular conversion constants are not ingested here.
    """
    xlsx_path = Path(xlsx_path)

    if not xlsx_path.exists():
        raise FileNotFoundError(f"Emission parameter workbook not found: {xlsx_path}")

    if xlsx_path.suffix.lower() != ".xlsx":
        raise ValueError(f"Emission parameter workbook must be an .xlsx file. Got: {xlsx_path}")

    if xlsx_path.name.lower() != EXPECTED_FILENAME.lower():
        raise ValueError(
            f"Emission parameter ingester only accepts '{EXPECTED_FILENAME}'. "
            f"Got: {xlsx_path.name}"
        )

    # Load the whole sheet without treating any row as the header.
    #
    # This is important because the sheet is not one rectangular table.
    # Instead, it is four vertically stacked tables with category headings.
    df = pd.read_excel(
        xlsx_path,
        sheet_name=SHEET_FIRE_CATEGORY_PARAMS,
        engine="openpyxl",
        header=None,
        dtype=object,
    )

    warnings: list[str] = []

    # Find the category block heading rows.
    # These are rows where column A contains one of the known category labels.
    block_starts = _find_category_block_starts(df)

    # Ensure all four expected combustion-relevant categories are present.
    found_categories = {category for _, category in block_starts}
    missing_categories = sorted(REQUIRED_FIRE_SPREAD_CATEGORIES - found_categories)

    if missing_categories:
        raise ValueError(
            f"[{SHEET_FIRE_CATEGORY_PARAMS}] Missing required fire category block(s): "
            + ", ".join(missing_categories)
        )

    rows: list[EmissionParameterRow] = []

        # Parse each block independently.
    for block_i, (start_idx, fire_spread_category) in enumerate(block_starts):
        # The end of the current block is the row before the next block.
        # For the final block, use the end of the worksheet.
        if block_i + 1 < len(block_starts):
            end_idx = block_starts[block_i + 1][0]
        else:
            end_idx = len(df)

        block_rows = _parse_category_block(
            df=df,
            start_idx=start_idx,
            end_idx=end_idx,
            fire_spread_category=fire_spread_category,
            warnings=warnings,
        )

        rows.extend(block_rows)

    # Validate the combined tidy rows.
    #
    # Some checks, especially species-fraction sums, require the complete
    # dataset rather than a single block at a time.
    _validate_emission_parameter_rows(rows, warnings)

    return EmissionParameterWorkbook(
        rows=rows,
        warnings=warnings,
    )


# -----------------------------------------------------------------------------
# Block parsing
# -----------------------------------------------------------------------------

def _find_category_block_starts(df: pd.DataFrame) -> list[tuple[int, str]]:
    """
    Find category block heading rows in column A.

    Returns
    -------
    list of tuples:
        (zero_based_dataframe_row_index, fire_spread_category)

    Notes
    -----
    The workbook should now use model-facing category names directly, e.g.
        single_item
        within_room
        multiple_rooms
        entire_dwelling

    Therefore no category alias mapping is required.
    """
    block_starts: list[tuple[int, str]] = []

    for idx, raw_value in df.iloc[:, 0].items():
        if _is_blank(raw_value):
            continue

        category = _clean_key(raw_value)

        if category in VALID_FIRE_SPREAD_CATEGORIES:
            block_starts.append((int(idx), category))

    if not block_starts:
        raise ValueError(
            f"[{SHEET_FIRE_CATEGORY_PARAMS}] No fire category blocks found in column A."
        )

    block_starts.sort(key=lambda x: x[0])

    seen: set[str] = set()
    duplicates: list[str] = []

    for _, category in block_starts:
        if category in seen:
            duplicates.append(category)
        seen.add(category)

    if duplicates:
        raise ValueError(
            f"[{SHEET_FIRE_CATEGORY_PARAMS}] Duplicate category block(s): "
            + ", ".join(sorted(set(duplicates)))
        )

    return block_starts


def _parse_category_block(
    *,
    df: pd.DataFrame,
    start_idx: int,
    end_idx: int,
    fire_spread_category: str,
    warnings: list[str],
) -> list[EmissionParameterRow]:
    """
    Parse one fire-spread-category block.

    Parameters
    ----------
    df:
        The full worksheet dataframe with no header row.

    start_idx:
        Zero-based dataframe row containing the category heading.

    end_idx:
        Zero-based dataframe row where this block stops.

    fire_spread_category:
        Canonical category label read from the workbook, e.g. single_item.

    warnings:
        List of non-blocking warning strings to append to.

    Returns
    -------
    list[EmissionParameterRow]
        One tidy row for each parameter in this category block.
    """

    # The header row should be immediately below the category heading.
    header_idx = start_idx + 1

    if header_idx >= len(df):
        raise ValueError(
            f"[{SHEET_FIRE_CATEGORY_PARAMS}] Category '{category_input}' has no header row."
        )

    # Read the first five columns from the header row.
    # Expected:
    #   fire_emission_parameter | min | default | max | notes
    raw_headers = [
        _clean_header(df.iat[header_idx, col_idx])
        for col_idx in range(5)
    ]

    expected_headers = [
        "fire_emission_parameter",
        "min",
        "default",
        "max",
        "notes",
    ]

    if raw_headers != expected_headers:
        raise ValueError(
            f"[{SHEET_FIRE_CATEGORY_PARAMS}] Invalid header row for category "
            f"'{category_input}' at Excel row {header_idx + 1}.\n"
            f"Expected: {expected_headers}\n"
            f"Found:    {raw_headers}"
        )

    rows: list[EmissionParameterRow] = []
    seen_parameters: set[str] = set()

    # Parameter rows begin after the header row.
    for row_idx in range(header_idx + 1, end_idx):
        # Excel row number is dataframe index + 1.
        excel_row_number = row_idx + 1

        raw_parameter = df.iat[row_idx, 0]

        # Blank rows are ignored. This allows spacing rows within or after
        # the block without making the ingest fail.
        if _is_blank(raw_parameter):
            continue

        fire_emission_parameter = str(raw_parameter).strip()

        # Stop defensively if another category appears unexpectedly inside
        # the block range. In normal use this should not happen because
        # end_idx is based on the next category heading.
        if _clean_key(fire_emission_parameter) in VALID_FIRE_SPREAD_CATEGORIES:
            break

        if fire_emission_parameter in seen_parameters:
            raise ValueError(
                f"[{SHEET_FIRE_CATEGORY_PARAMS}] Duplicate parameter "
                f"'{fire_emission_parameter}' in category '{category_input}'."
            )

        seen_parameters.add(fire_emission_parameter)

        # Parse the parameter name into useful metadata.
        (
            parameter_type,
            emission_species,
            ventilation_condition,
        ) = _parse_parameter_metadata(fire_emission_parameter)

        raw_min = df.iat[row_idx, 1]
        raw_default = df.iat[row_idx, 2]
        raw_max = df.iat[row_idx, 3]
        raw_notes = df.iat[row_idx, 4]

        (
            is_applicable,
            value_min,
            value_default,
            value_max,
        ) = _parse_value_triplet(
            raw_min=raw_min,
            raw_default=raw_default,
            raw_max=raw_max,
            category_input=fire_spread_category,
            fire_emission_parameter=fire_emission_parameter,
            excel_row_number=excel_row_number,
            warnings=warnings,
        )

        notes = None if _is_blank(raw_notes) else str(raw_notes).strip()

        rows.append(
            EmissionParameterRow(
                fire_spread_category=fire_spread_category,
                fire_emission_parameter=fire_emission_parameter,
                parameter_type=parameter_type,
                emission_species=emission_species,
                ventilation_condition=ventilation_condition,
                is_applicable=is_applicable,
                value_min=value_min,
                value_default=value_default,
                value_max=value_max,
                notes=notes,
                source_sheet=SHEET_FIRE_CATEGORY_PARAMS,
                source_table=fire_spread_category,
                input_row_number=excel_row_number,
            )
        )

    # Check that the current expected rows have not been accidentally deleted.
    #
    # A row is allowed to be N/A for a category, but the row should still exist
    # in this first workbook design because all blocks have the same row layout.
    missing_parameters = sorted(CURRENT_REQUIRED_PARAMETERS - seen_parameters)

    if missing_parameters:
        raise ValueError(
            f"[{SHEET_FIRE_CATEGORY_PARAMS}] Category '{category_input}' is missing "
            f"required parameter row(s): "
            + ", ".join(missing_parameters)
        )

    return rows


def _parse_parameter_metadata(
    fire_emission_parameter: str,
) -> tuple[str, Optional[str], Optional[str]]:
    """
    Classify a fire_emission_parameter row.

    Returns
    -------
    parameter_type, emission_species, ventilation_condition

    parameter_type is currently one of:
        species_emission_factor
        model_control_parameter

    Notes
    -----
    Species emission factors use a generic parser so that later rows such as
    CH4_emission_factor_overventilated can be added without changing the code.
    """
    match = SPECIES_EMISSION_FACTOR_RE.match(fire_emission_parameter)

    if match:
        species = match.group("species").upper()
        ventilation_condition = _clean_key(match.group("ventilation_condition"))

        if ventilation_condition not in VALID_VENTILATION_CONDITIONS:
            raise ValueError(
                f"Invalid ventilation condition in parameter "
                f"'{fire_emission_parameter}'. "
                f"Allowed: {sorted(VALID_VENTILATION_CONDITIONS)}"
            )

        return (
            "species_emission_factor",
            species,
            ventilation_condition,
        )

    if fire_emission_parameter in KNOWN_MODEL_CONTROL_PARAMETERS:
        return (
            "model_control_parameter",
            None,
            None,
        )

    raise ValueError(
        f"Unknown fire_emission_parameter: '{fire_emission_parameter}'. "
        "Expected a known model-control parameter or a species emission factor "
        "named like '{species}_emission_factor_{ventilation_condition}'."
    )


def _parse_value_triplet(
    *,
    raw_min: Any,
    raw_default: Any,
    raw_max: Any,
    category_input: str,
    fire_emission_parameter: str,
    excel_row_number: int,
    warnings: list[str],
) -> tuple[int, Optional[float], Optional[float], Optional[float]]:
    """
    Parse min/default/max values for one row.

    Rules
    -----
    1. If min/default/max are all blank or N/A-like, the row is treated as
       not applicable for this fire-spread category.
    2. If default is blank or N/A, but min or max contains a numeric value,
       this is an error because the deterministic model requires default.
    3. If default is numeric, the row is applicable.
    4. min and max may be blank or N/A for an applicable row, because they are
       only required for testing / sensitivity analysis.
    5. If min/default/max are all available, require min <= default <= max.
    """
    raw_values = {
        "min": raw_min,
        "default": raw_default,
        "max": raw_max,
    }

    # Classify each cell first.
    #
    # This avoids confusing three different meanings:
    #   - blank cell: empty / not supplied
    #   - N/A cell: explicitly not applicable
    #   - numeric cell: actual model parameter value
    cell_is_blank_or_na = {
        col: _is_blank(value) or _is_explicit_na(value)
        for col, value in raw_values.items()
    }

    # If all three cells are blank or N/A, this parameter is intentionally not
    # applicable for this fire-spread category.
    #
    # This matches the current workbook design, where non-relevant parameters
    # are allowed to be left blank.
    if all(cell_is_blank_or_na.values()):
        return 0, None, None, None

    # The row is only applicable if the deterministic default value is present.
    # If min/max are present without default, the row would be ambiguous.
    if cell_is_blank_or_na["default"]:
        raise ValueError(
            f"[{SHEET_FIRE_CATEGORY_PARAMS}] Missing default value at Excel row "
            f"{excel_row_number} ({category_input} / {fire_emission_parameter}). "
            "If this parameter is not applicable, leave min/default/max all blank "
            "or set all three to N/A."
        )

    # Coerce the deterministic default.
    #
    # This is the value the general model will normally use.
    value_default = _coerce_numeric_cell(
        raw_values["default"],
        column="default",
        category_input=category_input,
        fire_emission_parameter=fire_emission_parameter,
        excel_row_number=excel_row_number,
    )

    # min is optional.
    #
    # It is only required later for testing / sensitivity analysis, so blank or
    # N/A is allowed here. We keep a warning so that missing ranges remain
    # visible during dry-run.
    if cell_is_blank_or_na["min"]:
        value_min = None
        warnings.append(
            f"[{SHEET_FIRE_CATEGORY_PARAMS}] Missing min value at Excel row "
            f"{excel_row_number} ({category_input} / {fire_emission_parameter})."
        )
    else:
        value_min = _coerce_numeric_cell(
            raw_values["min"],
            column="min",
            category_input=category_input,
            fire_emission_parameter=fire_emission_parameter,
            excel_row_number=excel_row_number,
        )

    # max is also optional for the same reason.
    if cell_is_blank_or_na["max"]:
        value_max = None
        warnings.append(
            f"[{SHEET_FIRE_CATEGORY_PARAMS}] Missing max value at Excel row "
            f"{excel_row_number} ({category_input} / {fire_emission_parameter})."
        )
    else:
        value_max = _coerce_numeric_cell(
            raw_values["max"],
            column="max",
            category_input=category_input,
            fire_emission_parameter=fire_emission_parameter,
            excel_row_number=excel_row_number,
        )

    # If both range endpoints are present, check the ordering.
    #
    # If either min or max is missing, we cannot check the full range, but the
    # deterministic default is still valid.
    if value_min is not None and value_max is not None:
        if not (value_min <= value_default <= value_max):
            raise ValueError(
                f"[{SHEET_FIRE_CATEGORY_PARAMS}] Expected min <= default <= max "
                f"at Excel row {excel_row_number} "
                f"({category_input} / {fire_emission_parameter}). "
                f"Found: {value_min} <= {value_default} <= {value_max}"
            )

    return 1, value_min, value_default, value_max


# -----------------------------------------------------------------------------
# Cross-row validation
# -----------------------------------------------------------------------------

def _validate_emission_parameter_rows(
    rows: list[EmissionParameterRow],
    warnings: list[str],
) -> None:
    """
    Validate the complete tidy parameter table.

    Some checks need all rows together, especially:
        - duplicate category/parameter combinations
        - fraction bounds
        - char_formation_factor = 1
        - species emission factor sums <= 1
    """
    if not rows:
        raise ValueError("No emission parameter rows were parsed from the workbook.")

    _validate_unique_category_parameter(rows)
    _validate_fraction_bounds(rows)
    _validate_char_formation_factor(rows)
    _validate_species_fraction_sums(rows)

    # Warn if a category has zero applicable rows.
    # This should never happen for the four combustion-relevant categories.
    for category in sorted({r.fire_spread_category for r in rows}):
        applicable_count = sum(
            1
            for r in rows
            if r.fire_spread_category == category and r.is_applicable == 1
        )

        if applicable_count == 0:
            warnings.append(
                f"No applicable emission parameters found for category '{category}'."
            )


def _validate_unique_category_parameter(rows: list[EmissionParameterRow]) -> None:
    """
    Ensure that each category/parameter combination appears only once.

    This matches the UNIQUE constraint expected in the database and prevents
    ambiguous lookups during the later emissions calculation.
    """
    seen: set[tuple[str, str]] = set()
    duplicates: list[tuple[str, str]] = []

    for row in rows:
        key = (row.fire_spread_category, row.fire_emission_parameter)

        if key in seen:
            duplicates.append(key)

        seen.add(key)

    if duplicates:
        dup_text = ", ".join(
            f"{category}/{parameter}"
            for category, parameter in duplicates[:20]
        )

        raise ValueError(
            "Duplicate fire emission parameter rows found: " + dup_text
        )


def _validate_fraction_bounds(rows: list[EmissionParameterRow]) -> None:
    """
    Check that applicable parameter values are within [0, 1].

    At present all ingested emission parameters are fractions or positions on
    the unit interval. The only special case is char_formation_factor, which is
    currently a neutral placeholder and is separately forced to 1.
    """
    value_columns = [
        ("value_min", "min"),
        ("value_default", "default"),
        ("value_max", "max"),
    ]

    for row in rows:
        if row.is_applicable == 0:
            continue

        for attr_name, label in value_columns:
            value = getattr(row, attr_name)

            # min/max may be blank for sensitivity analysis, so NULL is allowed.
            if value is None:
                continue

            if value < 0 or value > 1:
                raise ValueError(
                    f"[{SHEET_FIRE_CATEGORY_PARAMS}] Value outside [0, 1] for "
                    f"{row.fire_spread_category} / "
                    f"{row.fire_emission_parameter} / {label}: {value}"
                )


def _validate_char_formation_factor(rows: list[EmissionParameterRow]) -> None:
    """
    Enforce char_formation_factor = 1 for the first-pass model.

    Rationale
    ---------
    In this first implementation, char formation is only a neutral placeholder.
    Therefore:
        - default must be 1 wherever the row is applicable
        - min/max, if supplied, must also be 1

    This prevents the sensitivity inputs from accidentally making the first
    deterministic model behave as if a char sub-model already exists.
    """
    for row in rows:
        if row.fire_emission_parameter != "char_formation_factor":
            continue

        if row.is_applicable == 0:
            raise ValueError(
                f"char_formation_factor should not be N/A for "
                f"{row.fire_spread_category}. "
                "Use 1 as the neutral placeholder."
            )

        if row.value_default != 1:
            raise ValueError(
                f"char_formation_factor default must be 1 for "
                f"{row.fire_spread_category}. "
                f"Found: {row.value_default}"
            )

        if row.value_min is not None and row.value_min != 1:
            raise ValueError(
                f"char_formation_factor min must be 1 for "
                f"{row.fire_spread_category}. "
                f"Found: {row.value_min}"
            )

        if row.value_max is not None and row.value_max != 1:
            raise ValueError(
                f"char_formation_factor max must be 1 for "
                f"{row.fire_spread_category}. "
                f"Found: {row.value_max}"
            )


def _validate_species_fraction_sums(rows: list[EmissionParameterRow]) -> None:
    """
    Ensure species carbon fractions do not sum above 1.

    This is deliberately generic.

    Instead of checking only:
        CO2 + CO <= 1

    this groups all rows where:
        parameter_type == "species_emission_factor"

    and checks the sum by:
        fire_spread_category
        ventilation_condition
        value column

    That way, if we later add CH4, soot, VOCs, etc., this validation still
    checks the full carbon-fraction budget without changing the calculation.
    """
    value_columns = [
        ("value_min", "min"),
        ("value_default", "default"),
        ("value_max", "max"),
    ]

    # Group rows by category and ventilation condition.
    grouped: dict[tuple[str, str], list[EmissionParameterRow]] = {}

    for row in rows:
        if row.is_applicable == 0:
            continue

        if row.parameter_type != "species_emission_factor":
            continue

        if row.ventilation_condition is None:
            raise ValueError(
                f"Species emission factor '{row.fire_emission_parameter}' is "
                "missing ventilation_condition metadata."
            )

        key = (row.fire_spread_category, row.ventilation_condition)
        grouped.setdefault(key, []).append(row)

    # Sum each group for min/default/max separately.
    for (category, ventilation_condition), group_rows in grouped.items():
        for attr_name, label in value_columns:
            values = [
                getattr(row, attr_name)
                for row in group_rows
                if getattr(row, attr_name) is not None
            ]

            # If min or max values are missing for some rows, skip the sum for
            # that column. The deterministic default sum is still checked.
            if len(values) != len(group_rows):
                continue

            total = float(sum(values))

            # Allow a tiny tolerance for floating point representation.
            if total > 1.0000001:
                included_parameters = ", ".join(
                    row.fire_emission_parameter
                    for row in group_rows
                )

                raise ValueError(
                    f"Species emission fractions sum to more than 1 for "
                    f"{category} / {ventilation_condition} / {label}. "
                    f"Sum = {total}. "
                    f"Rows included: {included_parameters}"
                )


# -----------------------------------------------------------------------------
# DB ingest
# -----------------------------------------------------------------------------

def ingest_emission_parameters_pandas(
    *,
    db_path: str | Path,
    xlsx_path: str | Path,
    mode: str = "replace_all",
) -> dict[str, Any]:
    """
    Ingest the emission parameter workbook into fire_db.

    This is the core DB-write function, similar to:
        vocab.ingest_mapping_list_pandas(...)

    Parameters
    ----------
    db_path:
        Path to fire_db.

    xlsx_path:
        Path to emission_param.xlsx.

    mode:
        Currently supports:
            replace_all

    Behaviour
    ---------
    - Validates the workbook before writing.
    - Deletes only rows from fire_emission_parameter_mapping.
    - Does not touch fire_event_parameter_input, fire_events, or warnings.
    - Records the workbook in sources.
    - Attempts to record the ingest in ingest_log.
    """
    if mode != "replace_all":
        raise ValueError("mode must be 'replace_all' for emission parameter ingest.")

    db_path = Path(db_path)
    xlsx_path = Path(xlsx_path)

    started_utc = utc_now_iso()

    # First parse and validate the workbook before starting the DB transaction.
    # This avoids deleting the old parameter table if the new workbook is invalid.
    workbook = read_emission_parameters_xlsx_pandas(xlsx_path)

    conn = db_connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA foreign_keys = ON;")

        # Check the required destination tables before attempting any writes.
        _validate_destination_schema(conn)

        conn.execute("BEGIN;")

        # Replace only the emission-parameter mapping table.
        # Other fire-event input and model-facing tables are intentionally left
        # untouched.
        deleted_rows = _delete_existing_emission_parameter_rows(conn)

        source_id = _insert_source_row(conn, xlsx_path)

        _insert_emission_parameter_rows(
            conn=conn,
            source_id=source_id,
            rows=workbook.rows,
            created_at_utc=utc_now_iso(),
        )

        conn.commit()

        # Write ingest log after the main commit.
        # This is nice-to-have audit metadata, so the main ingest should not
        # fail if logging itself fails for any reason.
        try:
            record_ingest_run(
                conn,
                IngestLogEntry(
                    source_id=source_id,
                    data_source_type=SOURCE_TYPE,
                    action="ingest",
                    status="success",
                    message=(
                        f"Imported emission parameter workbook with "
                        f"{len(workbook.rows)} parameter rows."
                    ),
                    file_path=str(xlsx_path),
                    file_name=xlsx_path.name,
                    started_utc=started_utc,
                    finished_utc=utc_now_iso(),
                    rows_inserted=len(workbook.rows),
                    rows_deleted=deleted_rows,
                ),
            )
            conn.commit()
        except Exception:
            # Do not roll back a successful parameter ingest because of a log
            # failure. This mirrors the general pattern used elsewhere.
            pass

        return {
            "file": str(xlsx_path),
            "mode": mode,
            "source_id": source_id,
            "rows_inserted": len(workbook.rows),
            "rows_deleted": deleted_rows,
            "warnings": workbook.warnings,
        }

    except Exception as exc:
        conn.rollback()

        # Attempt to record a failed ingest, but do not mask the original error.
        try:
            record_ingest_run(
                conn,
                IngestLogEntry(
                    data_source_type=SOURCE_TYPE,
                    action="ingest",
                    status="failed",
                    message=str(exc),
                    file_path=str(xlsx_path),
                    file_name=xlsx_path.name,
                    started_utc=started_utc,
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
# DB helper functions
# -----------------------------------------------------------------------------

def _validate_destination_schema(conn: sqlite3.Connection) -> None:
    """
    Check that fire_db contains the tables required by this ingest.

    The ingester does not create schema objects itself.
    If this fails, rerun:
        python -m scripts.fire.init_fire_db --profile tom --db fire_db
    after adding the table definition to init_fire_db.py.
    """
    existing_tables = _list_tables(conn)

    required_tables = {
        TABLE_SOURCES,
        TABLE_INGEST_LOG,
        TABLE_EMISSION_PARAMS,
    }

    missing = sorted(required_tables - existing_tables)

    if missing:
        raise RuntimeError(
            "fire_db is missing required table(s): "
            + ", ".join(missing)
            + ". Run or update scripts.fire.init_fire_db first."
        )


def _insert_source_row(
    conn: sqlite3.Connection,
    xlsx_path: str | Path,
) -> str:
    """
    Insert one source row for this emission parameter workbook import.

    Source rows are retained as an audit trail, even though the parameter table
    itself is replace-all.
    """
    xlsx_path = Path(xlsx_path)
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
            xlsx_path.name,
            str(xlsx_path),
            None,
            None,
            utc_now_iso(),
            "Controlled fire emission parameter workbook.",
        ),
    )

    return source_id


def _delete_existing_emission_parameter_rows(
    conn: sqlite3.Connection,
) -> int:
    """
    Delete only the existing emission parameter rows.

    This is the overwrite behaviour for this ingest. It does not affect:
        - fire_event_parameter_input
        - fire_input_value_mapping
        - fire_ignition_item_mapping
        - fire_events
        - fire_event_warnings
        - inventory snapshot tables
    """
    cur = conn.execute(f"DELETE FROM {TABLE_EMISSION_PARAMS};")
    return cur.rowcount if cur.rowcount != -1 else 0


def _insert_emission_parameter_rows(
    *,
    conn: sqlite3.Connection,
    source_id: str,
    rows: list[EmissionParameterRow],
    created_at_utc: str,
) -> None:
    """
    Insert validated emission parameter rows into fire_db.
    """
    if not rows:
        return

    payload = [
        (
            source_id,
            r.fire_spread_category,
            r.fire_emission_parameter,
            r.parameter_type,
            r.emission_species,
            r.ventilation_condition,
            r.is_applicable,
            r.value_min,
            r.value_default,
            r.value_max,
            r.notes,
            r.source_sheet,
            r.source_table,
            r.input_row_number,
            created_at_utc,
        )
        for r in rows
    ]

    conn.executemany(
        f"""
        INSERT INTO {TABLE_EMISSION_PARAMS} (
            source_id,
            fire_spread_category,
            fire_emission_parameter,
            parameter_type,
            emission_species,
            ventilation_condition,
            is_applicable,
            value_min,
            value_default,
            value_max,
            notes,
            source_sheet,
            source_table,
            input_row_number,
            created_at_utc
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )


def count_existing_emission_parameter_rows(
    db_path: str | Path,
) -> dict[str, int]:
    """
    Count current rows in the emission parameter mapping table.

    This is useful for the dispatcher-facing ingest_emission_parameters.py
    wrapper when reporting the dry-run plan.
    """
    db_path = Path(db_path)

    con = sqlite3.connect(str(db_path))

    try:
        cur = con.cursor()

        if not _table_exists(cur, TABLE_EMISSION_PARAMS):
            return {
                "rows_fire_emission_parameter_mapping": 0,
                "rows_total": 0,
            }

        row = cur.execute(
            f"SELECT COUNT(*) FROM {TABLE_EMISSION_PARAMS};"
        ).fetchone()

        n_rows = int(row[0]) if row and row[0] is not None else 0

        return {
            "rows_fire_emission_parameter_mapping": n_rows,
            "rows_total": n_rows,
        }

    finally:
        con.close()


# -----------------------------------------------------------------------------
# Small cleaning / coercion helpers
# -----------------------------------------------------------------------------

def _require_cols(df: pd.DataFrame, required: list[str], sheet: str) -> None:
    """
    Check that a dataframe contains the required columns.

    Included for consistency with vocab.py, although the block parser mostly
    checks the header row manually because the workbook is not one rectangular
    table.
    """
    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(
            f"Sheet '{sheet}' missing required columns: {missing}. "
            f"Found: {list(df.columns)}"
        )


def _coerce_numeric_cell(
    value: Any,
    *,
    column: str,
    category_input: str,
    fire_emission_parameter: str,
    excel_row_number: int,
) -> float:
    """
    Convert one Excel cell to float.

    Raises a detailed error if the cell contains non-numeric text.
    """
    try:
        if isinstance(value, bool):
            raise ValueError("boolean is not numeric")

        return float(value)

    except Exception as exc:
        raise ValueError(
            f"[{SHEET_FIRE_CATEGORY_PARAMS}] Column '{column}' must contain a "
            f"numeric value at Excel row {excel_row_number} "
            f"({category_input} / {fire_emission_parameter}). "
            f"Found: {value!r}"
        ) from exc


def _is_blank(value: Any) -> bool:
    """
    Treat None, NaN, and empty/whitespace strings as blank.

    This helper is intentionally permissive because Excel and pandas represent
    empty cells in several slightly different ways.
    """
    if value is None:
        return True

    try:
        if pd.isna(value):
            return True
    except Exception:
        pass

    return str(value).strip().lower() in NULL_LIKE_TEXT


def _is_explicit_na(value: Any) -> bool:
    """
    Return True only for explicit N/A-style text.

    This is kept separate from _is_blank() because a blank cell means
    "missing value", whereas N/A means "parameter intentionally not applicable".
    """
    if value is None:
        return False

    try:
        if pd.isna(value):
            return False
    except Exception:
        pass

    return str(value).strip().lower() in NA_LIKE_TEXT


def _clean_key(value: Any) -> str:
    """
    Clean a canonical key-like value.

    This follows the general project style:
        - trim whitespace
        - lower case
        - replace spaces with underscores
    """
    return str(value).strip().lower().replace(" ", "_")


def _clean_header(value: Any) -> str:
    """
    Clean an Excel header value for strict comparison.

    Header matching is case-insensitive and ignores leading/trailing spaces.
    """
    return _clean_key(value)


def _list_tables(conn: sqlite3.Connection) -> set[str]:
    """
    Return all table names in the connected SQLite database.
    """
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table';
        """
    ).fetchall()

    return {str(row["name"] if isinstance(row, sqlite3.Row) else row[0]) for row in rows}


def _table_exists(cur: sqlite3.Cursor, table_name: str) -> bool:
    """
    Return True if a table exists in the connected SQLite database.
    """
    row = cur.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?;
        """,
        (table_name,),
    ).fetchone()

    return row is not None