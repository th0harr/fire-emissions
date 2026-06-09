# scripts/fire/ingest_emission_parameters.py
from __future__ import annotations

from pathlib import Path
from typing import Any

from scripts.fire import emission_parameters


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Controlled input workbook name.
#
# The shared ingest dispatcher will resolve the raw/config folder from:
#
#   config/local_paths.yaml
#
# using:
#
#   --type emissions
#
# This ingester then looks for this exact file inside that resolved folder.
EXPECTED_FILENAME = emission_parameters.EXPECTED_FILENAME


# -----------------------------------------------------------------------------
# Public dispatcher interface
# -----------------------------------------------------------------------------

def scan_inputs(raw_dir: Path) -> list[Path]:
    """
    Strict scanner for the fire emission parameter workbook.

    The current policy is deliberately simple:

        - there should be exactly one controlled workbook
        - it should be called emission_param.xlsx
        - it should live in the raw/config path resolved from local_paths.yaml

    This mirrors the vocabulary ingester style, where mapping_list.xlsx is
    treated as a controlled configuration workbook rather than a folder of
    many raw data files.

    Parameters
    ----------
    raw_dir:
        Raw/config directory resolved by scripts.ingest using local_paths.yaml.

    Returns
    -------
    list[Path]
        A single-item list containing the emission parameter workbook path.

    Raises
    ------
    FileNotFoundError
        If emission_param.xlsx is not found in the resolved raw/config folder.
    """
    raw_dir = Path(raw_dir)

    # Build the expected workbook path from the resolved raw directory.
    # The user should not normally need to provide this path manually on the CLI.
    xlsx_path = raw_dir / EXPECTED_FILENAME

    # Fail early if the controlled workbook is missing.
    # This avoids a confusing later error inside pandas/openpyxl.
    if not xlsx_path.exists():
        raise FileNotFoundError(
            f"Emission parameter workbook not found: {xlsx_path}\n"
            f"Expected exactly one file named '{EXPECTED_FILENAME}' in the "
            f"resolved emissions raw/config directory."
        )

    return [xlsx_path]


def plan(
    db_path: Path,
    raw_dir: Path,
    input_files: list[Path],
) -> dict[str, Any]:
    """
    Plan emission parameter ingestion.

    This is the dry-run planning step called by scripts.ingest before any
    database lock or write operation is attempted.

    Current policy
    --------------
    The emission parameter workbook is treated as the current authoritative
    model-configuration workbook. Therefore, if the file exists, it is always
    considered ingestable/current rather than being skipped because a previous
    version was already imported.

    This matches the behaviour we want during development:
        - edit emission_param.xlsx
        - run dry-run
        - run apply
        - replace only the emission parameter mapping table

    Parameters
    ----------
    db_path:
        Path to fire_db resolved by scripts.ingest.

    raw_dir:
        Raw/config directory resolved by scripts.ingest.

        This is not directly needed here once input_files has been supplied,
        but it is kept in the signature for compatibility with the shared
        dispatcher interface.

    input_files:
        Input files supplied by either:
            - scan_inputs(raw_dir), when using --scan
            - the --file argument, when using a single explicit file

    Returns
    -------
    dict[str, Any]
        Dictionary in the shape expected by scripts.ingest, including:
            - new
            - already_ingested

        Additional details are included for future debugging, although the
        current dispatcher only prints the main counts.
    """
    _ = Path(raw_dir)  # kept for signature consistency with other ingesters

    # Enforce the controlled-workbook policy:
    #   exactly one file, correct filename, correct extension, file exists.
    xlsx_path = _validate_single_emission_parameter_file(input_files)

    # Read and validate the workbook during dry-run.
    #
    # This is slightly more cautious than the simplest vocab ingester pattern.
    # It means that:
    #   python -m scripts.ingest ... --type emissions --scan
    #
    # will catch workbook problems before the user adds --apply.
    workbook = emission_parameters.read_emission_parameters_xlsx_pandas(xlsx_path)

    # Count the rows currently in fire_emission_parameter_mapping.
    # This gives the user a useful indication of what will be replaced.
    counts = emission_parameters.count_existing_emission_parameter_rows(Path(db_path))
    already = int(counts.get("rows_total", 0))

    # Return a plan in the format expected by the shared dispatcher.
    #
    # The workbook is always listed as "new" because this is a replace-current
    # configuration ingest, not a one-time raw data ingest.
    return {
        "new": [xlsx_path],
        "already_ingested": already,
        "details": {
            "file": str(xlsx_path),
            "mode": "replace_all",
            "rows_parsed": len(workbook.rows),
            "warnings": workbook.warnings,
            **counts,
        },
    }


def prune_preview(
    db_path: Path,
    raw_dir: Path,
) -> list[Any]:
    """
    Preview prune candidates.

    There is no separate pruning concept for emission parameters.

    Rationale
    ---------
    The emission parameter workbook represents the current parameter
    configuration. Obsolete rows are handled implicitly when ingest_apply()
    runs in replace_all mode.

    Therefore:
        - --prune does not identify missing source files
        - old parameter rows are not selectively pruned
        - the whole parameter mapping table is replaced during apply

    Returns
    -------
    list[Any]
        Always an empty list.
    """
    _ = db_path, raw_dir
    return []


def prune_apply(
    db_path: Path,
    raw_dir: Path,
) -> dict[str, Any]:
    """
    Apply pruning.

    This is not implemented for emission parameters because obsolete values are
    removed by ingest_apply() when running in replace_all mode.

    The function still exists because scripts.ingest expects every ingester
    module to provide it.

    Returns
    -------
    dict[str, Any]
        Small summary confirming that pruning is not applicable.
    """
    _ = db_path, raw_dir

    return {
        "rows_deleted": 0,
        "note": "not applicable; emission parameters are replaced during ingest_apply()",
    }


def ingest_apply(
    db_path: Path,
    raw_dir: Path,
    new_files: list[Path],
) -> dict[str, Any]:
    """
    Apply emission parameter ingestion.

    This function is called by scripts.ingest only when --apply is used.

    Behaviour
    ---------
    The core ingest function:

        emission_parameters.ingest_emission_parameters_pandas(...)

    will:
        - validate the workbook
        - delete existing rows only from fire_emission_parameter_mapping
        - insert the new tidy parameter rows
        - record a source row
        - attempt to record an ingest_log row

    It will not touch:
        - fire_event_parameter_input
        - fire_input_value_mapping
        - fire_ignition_item_mapping
        - fire_events
        - fire_event_warnings
        - inventory snapshot tables

    Parameters
    ----------
    db_path:
        Path to fire_db resolved by scripts.ingest.

    raw_dir:
        Raw/config directory resolved by scripts.ingest.

        This is not directly needed once new_files has been supplied, but is
        retained for compatibility with the shared dispatcher interface.

    new_files:
        The files selected for ingestion by the plan step.

        For this ingester, this should contain exactly one file:
            emission_param.xlsx

    Returns
    -------
    dict[str, Any]
        Summary of the applied ingest.
    """
    _ = Path(raw_dir)  # kept for signature consistency with other ingesters

    # Validate that the dispatcher has passed exactly the controlled workbook.
    xlsx_path = _validate_single_emission_parameter_file(new_files)

    # Apply the actual ingest using the core module.
    #
    # This keeps this wrapper small and leaves the real parsing / validation /
    # DB-writing logic in scripts/fire/emission_parameters.py.
    ingest_summary = emission_parameters.ingest_emission_parameters_pandas(
        db_path=Path(db_path),
        xlsx_path=xlsx_path,
        mode="replace_all",
    )

    # Post-ingest counts are useful to confirm the final state of the table.
    counts = emission_parameters.count_existing_emission_parameter_rows(Path(db_path))

    return {
        "file": str(xlsx_path),
        "mode": "replace_all",
        **ingest_summary,
        **counts,
    }


# -----------------------------------------------------------------------------
# Private validation helpers
# -----------------------------------------------------------------------------

def _validate_single_emission_parameter_file(files: list[Path]) -> Path:
    """
    Enforce that emission parameter ingestion uses one controlled workbook.

    This mirrors the controlled-workbook checks in ingest_vocab.py.

    Rules
    -----
    - exactly one file must be supplied
    - the file must be called emission_param.xlsx
    - the file must have .xlsx extension
    - the file must exist

    Parameters
    ----------
    files:
        File list supplied by the dispatcher.

    Returns
    -------
    Path
        Validated workbook path.

    Raises
    ------
    ValueError
        If the wrong number of files, wrong filename, or wrong extension is
        supplied.

    FileNotFoundError
        If the supplied workbook path does not exist.
    """
    # This ingester is for a single controlled configuration workbook, not a
    # batch of raw files.
    if len(files) != 1:
        raise ValueError(
            f"Emission parameter ingester expects exactly one file: "
            f"{EXPECTED_FILENAME}. "
            f"Got {len(files)} file(s): {[str(p) for p in files]}"
        )

    p = Path(files[0])

    # Reject non-xlsx files.
    #
    # The current emission parameter workbook is not macro-enabled, so .xlsm
    # is not accepted here unless we later decide to support it deliberately.
    if p.suffix.lower() != ".xlsx":
        raise ValueError(
            f"Emission parameter ingester expects an .xlsx file. Got: {p}"
        )

    # Enforce the exact controlled filename.
    #
    # This prevents accidentally ingesting an old copy, a temporary workbook,
    # or a renamed sensitivity-analysis file by mistake.
    if p.name.lower() != EXPECTED_FILENAME.lower():
        raise ValueError(
            f"Emission parameter ingester only accepts "
            f"'{EXPECTED_FILENAME}'. Got: {p.name}"
        )

    # Finally check that the file actually exists.
    if not p.exists():
        raise FileNotFoundError(f"Input file does not exist: {p}")

    return p