"""
Build model-facing fire event input rows from staged fire input parameters.

This script transforms the raw/staged fire input tables into a single resolved,
model-facing event row.

Input tables:
    input_single_event
    fire_input_value_mapping
    fire_ignition_item_mapping
    inventory_*_snapshot

Output tables:
    fire_events
    fire_events_warning

Run from the project root, for example:

    # Dry run / preview
    python -m scripts.fire.build_fire_event_input --profile tom --db fire_db

    # Apply
    python -m scripts.fire.build_fire_event_input --profile tom --db fire_db --apply

Notes
-----
This script does NOT calculate carbon emissions.

It only resolves:
    - user-facing fire spread category -> canonical fire_spread_category
    - user-facing room description -> room_type
    - user-facing dwelling type -> canonical dwelling_type
    - ignition source -> optional item_combusted for single-item cases

The fire impact calculator will use fire_events later.
"""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.ingest_utils import IngestLogEntry, db_connect, record_ingest_run, utc_now_iso
from scripts.path_config import load_local_paths_config, resolve_db_path


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# This is a derived/model-facing build step, not a raw workbook ingest.
SOURCE_TYPE = "fire_events"

# Staging/configuration tables.
TABLE_SOURCES = "sources"
TABLE_INGEST_LOG = "ingest_log"

TABLE_SINGLE_EVENT_INPUT = "input_single_event"
TABLE_FIRE_INPUT_VALUE_MAPPING = "fire_input_value_mapping"
TABLE_FIRE_IGNITION_ITEM_MAPPING = "fire_ignition_item_mapping"

# Inventory snapshot lookup tables.
TABLE_INVENTORY_SNAPSHOT = "inventory_snapshot"
TABLE_INVENTORY_ROOM_SNAPSHOT = "inventory_room_snapshot"
TABLE_INVENTORY_ITEM_SNAPSHOT = "inventory_item_snapshot"
TABLE_INVENTORY_DWELLING_SIZE_SNAPSHOT = "inventory_dwelling_size_snapshot"

# Model-facing output tables.
TABLE_FIRE_EVENTS = "fire_events"
TABLE_FIRE_EVENT_WARNINGS = "fire_event_warnings"

# Fire parameters expected in the staged input table.
PARAM_FIRE_SPREAD = "fire_spread_category"
PARAM_ROOM_ORIGIN = "room_of_origin"
PARAM_FIRE_AREA = "fire_area_m2"
PARAM_SMOKE_HEAT_AREA = "smoke_heat_damage_area_m2"
PARAM_ROOM_SIZE = "room_of_origin_size_m2"
PARAM_DWELLING_SIZE = "dwelling_size_m2"
PARAM_DWELLING_TYPE = "dwelling_type"
PARAM_IGNITION_SOURCE_CATEGORY = "ignition_source_category"
PARAM_IGNITION_SOURCE = "ignition_source"
PARAM_INPUT_NOTES = "input_notes"

# Canonical spread classes accepted by fire_events.
VALID_FIRE_SPREAD_CLASSES = {
    "heat_smoke",
    "single_item",
    "within_room",
    "multiple_rooms",
    "entire_dwelling",
}

# Single-item statuses from fire_ignition_item_mapping.
VALID_SINGLE_ITEM_STATUSES = {
    "direct_inventory_item",
    "proxy_inventory_item",
    "invalid_single_item",
    "unmapped",
}

# Statuses that have a valid item_combusted value.
MODELLED_SINGLE_ITEM_STATUSES = {
    "direct_inventory_item",
    "proxy_inventory_item",
}

# Spread classes where room_of_origin is required for deterministic calculations.
ROOM_REQUIRED_SPREAD_CLASSES = {
    "within_room",
    "multiple_rooms",
}


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------

@dataclass
class StagedFireParameter:
    """
    One staged input parameter row.

    This mirrors input_single_event, but keeps the data in Python while
    resolving the model-facing input.
    """

    fire_parameter: str
    value_text: str | None
    value_numeric: float | None


@dataclass
class ResolvedFireEvent:
    """
    One model-facing fire event row.

    This is the in-memory version of fire_events.
    """

    source_id: str
    inventory_snapshot_id: int

    fire_spread_category_input: str
    fire_spread_category: str

    room_of_origin_input: str | None
    room_of_origin: str | None

    fire_area_m2: float | None
    smoke_heat_damage_area_m2: float | None
    room_of_origin_size_m2: float | None
    dwelling_size_m2: float | None

    dwelling_type_input: str | None
    dwelling_type: str | None

    ignition_source: str | None
    single_item_status: str | None
    item_combusted: str | None

    resolution_notes: str | None = None


@dataclass
class FireEventWarning:
    """
    One non-blocking warning generated during resolution.

    These warnings are inserted into fire_events_warning after the resolved
    event row has been inserted.
    """

    source_id: str
    warning_type: str
    fire_parameter: str | None
    warning_message: str
    warning_severity: str = "warning"


@dataclass
class ResolvePlan:
    """
    Dry-run result for building fire_events.

    The plan contains either:
        - a resolved event + optional warnings, or
        - blocking errors explaining why no event can be built.
    """

    source_id: str | None = None
    inventory_snapshot_id: int | None = None

    resolved_event: ResolvedFireEvent | None = None
    warnings: list[FireEventWarning] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)

    existing_event_rows: int = 0
    existing_current_event_rows: int = 0
    existing_warning_rows: int = 0

    @property
    def has_blocking_errors(self) -> bool:
        """Return True if the resolved event cannot be safely inserted."""
        return len(self.errors) > 0


@dataclass
class ResolveResult:
    """
    Result returned after applying the model-facing event build.
    """

    source_id: str
    inventory_snapshot_id: int
    rows_inserted_event: int
    rows_inserted_warnings: int

    overwrite: bool = False
    skipped_reason: str | None = None


# -----------------------------------------------------------------------------
# Public functions
# -----------------------------------------------------------------------------

def build_fire_event_input(
    db_path: str | Path,
    *,
    apply: bool = False,
    overwrite: bool = False,
) -> ResolvePlan | ResolveResult:
    """
    Build or preview the model-facing fire event row.

    Parameters
    ----------
    db_path:
        Path to fire_db/test_db.

    apply:
        If False, only previews the resolved event.
        If True, writes the resolved event into fire_events.

    overwrite:
        If False:
            Existing fire_events rows are kept.
            The current staged event is appended as a new row, provided the
            current staged source_id has not already been promoted.

        If True:
            Existing fire_events and fire_event_warnings rows are deleted before
            the current staged event is inserted.

    Notes
    -----
    This function deliberately does not modify the staging tables.

    The intended workflow is:

        ingest_fire_event_inputs.py
            refreshes input_single_event

        build_fire_event_input.py
            promotes the current staged case into fire_events

    This means the Excel workbook can contain one case at a time, while the
    model-facing fire_events table can accumulate multiple promoted cases.
    """
    db_path = Path(db_path)

    # Always build a dry-run plan first.
    # This gives one central place for validation and resolution logic.
    plan = plan_fire_event_input(db_path)

    # In preview mode, return the plan without writing anything.
    if not apply:
        return plan

    # If validation/resolution failed, do not allow a partial or unusable event
    # to be inserted into the model-facing table.
    if plan.has_blocking_errors:
        raise RuntimeError(
            "fire_events row was not built because blocking errors were found:\n"
            + "\n".join(f"- {e}" for e in plan.errors)
        )

    # Apply the resolved plan.
    # overwrite controls whether old model-facing rows are retained or cleared.
    return apply_fire_event_input(
        db_path=db_path,
        plan=plan,
        overwrite=overwrite,
    )


def plan_fire_event_input(db_path: str | Path) -> ResolvePlan:
    """
    Preview the fire event input resolution.

    This is read-only. It does not write to the database.
    """
    db_path = Path(db_path)

    conn = db_connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        plan = ResolvePlan()

        validate_required_schema(conn, plan.errors)

        if plan.has_blocking_errors:
            return plan

        # Existing rows are useful for dry-run reporting.
        plan.existing_event_rows = count_rows(conn, TABLE_FIRE_EVENTS)
        plan.existing_warning_rows = count_rows(conn, TABLE_FIRE_EVENT_WARNINGS)

        # Load the only staged source_id.
        source_id = get_single_staged_source_id(conn, plan.errors)

        if source_id is None:
            return plan

        plan.source_id = source_id

        # Check whether this staged source_id has already been promoted into
        # the model-facing fire_events table.
        #
        # This is useful for dry-run reporting and prevents confusion when
        # build_fire_event_input.py is run repeatedly after the same ingest.
        plan.existing_current_event_rows = count_fire_events_rows_for_source(
            conn=conn,
            source_id=source_id,
        )

        # Load current inventory snapshot.
        inventory_snapshot_id = get_current_inventory_snapshot_id(conn, plan.errors)

        if inventory_snapshot_id is None:
            return plan

        plan.inventory_snapshot_id = inventory_snapshot_id

        # Load staged input parameters into a dictionary keyed by fire_parameter.
        staged = load_staged_parameters(conn, source_id, plan.errors)

        if plan.has_blocking_errors:
            return plan

        # Resolve all staged values into one model-facing event row.
        event, warnings = resolve_event_from_staging(
            conn=conn,
            source_id=source_id,
            inventory_snapshot_id=inventory_snapshot_id,
            staged=staged,
            errors=plan.errors,
        )

        plan.resolved_event = event
        plan.warnings.extend(warnings)

        return plan

    finally:
        conn.close()


def apply_fire_event_input(
    db_path: str | Path,
    plan: ResolvePlan,
    *,
    overwrite: bool = False,
) -> ResolveResult:
    """
    Apply the resolved fire event build.

    Default behaviour
    -----------------
    Existing fire_events rows are kept.

    The current staged event is inserted as a new model-facing row, using the
    current staged source_id as the event identifier.

    If this exact source_id is already present in fire_events, the function
    performs a no-op rather than duplicating the event.

    Overwrite behaviour
    -------------------
    If overwrite=True, the model-facing fire event layer is deliberately cleared:

        fire_event_warnings
        fire_events

    The current staged event is then inserted as the only model-facing event.

    Notes
    -----
    This function should not delete from:
        - input_single_event
        - fire_input_value_mapping
        - fire_ignition_item_mapping
        - sources

    Those belong to the raw/staging ingest layer.

    This function only manages:
        - fire_events
        - fire_event_warnings
    """
    db_path = Path(db_path)

    if plan.resolved_event is None:
        raise RuntimeError("No resolved event is available to insert.")

    started = utc_now_iso()

    conn = db_connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        # -------------------------------------------------
        # Duplicate-build protection
        # -------------------------------------------------
        #
        # If the user runs build_fire_event_input.py twice after the same ingest,
        # the staged source_id will be identical.
        #
        # In normal append mode, we do not want to duplicate that row.
        #
        # To create a new fire event, the user should:
        #   1. edit the Excel input workbook
        #   2. run fire_event ingest again
        #   3. run build_fire_event_input again
        #
        # This creates a new source_id and therefore a new fire_events row.
        if not overwrite and fire_event_exists(
            conn=conn,
            source_id=plan.resolved_event.source_id,
        ):
            return ResolveResult(
                source_id=plan.resolved_event.source_id,
                inventory_snapshot_id=plan.resolved_event.inventory_snapshot_id,
                rows_inserted_event=0,
                rows_inserted_warnings=0,
                overwrite=False,
                skipped_reason=(
                    "This staged source_id already exists in fire_events. "
                    "Re-ingest the workbook to create a new source_id, or use "
                    "--overwrite to clear existing fire_events first."
                ),
            )

        conn.execute("BEGIN")

        # -------------------------------------------------
        # Optional overwrite
        # -------------------------------------------------
        #
        # This is the only place where promoted/model-facing fire events should
        # be deleted.
        #
        # fire_event_warnings is deleted first because it depends on fire_events.
        if overwrite:
            conn.execute(f"DELETE FROM {TABLE_FIRE_EVENT_WARNINGS};")
            conn.execute(f"DELETE FROM {TABLE_FIRE_EVENTS};")

        # -------------------------------------------------
        # Insert resolved model-facing event
        # -------------------------------------------------
        #
        # In normal mode, this appends the current staged case.
        # In overwrite mode, this inserts the first row after clearing the table.
        insert_fire_event_input(conn, plan.resolved_event)
        insert_fire_event_warnings(conn, plan.warnings)

        conn.commit()

        result = ResolveResult(
            source_id=plan.resolved_event.source_id,
            inventory_snapshot_id=plan.resolved_event.inventory_snapshot_id,
            rows_inserted_event=1,
            rows_inserted_warnings=len(plan.warnings),
            overwrite=overwrite,
            skipped_reason=None,
        )

        # -------------------------------------------------
        # Ingest/build log
        # -------------------------------------------------
        #
        # Logging happens after the main transaction.
        # If logging fails, the successfully inserted fire event is left intact.
        try:
            record_ingest_run(
                conn,
                IngestLogEntry(
                    source_id=plan.resolved_event.source_id,
                    data_source_type=SOURCE_TYPE,
                    action="build",
                    status="success",
                    message=(
                        f"Built fire_events row for source_id={plan.resolved_event.source_id}; "
                        f"overwrite={overwrite}; inserted {len(plan.warnings)} warning row(s)."
                    ),
                    started_utc=started,
                    finished_utc=utc_now_iso(),
                    rows_inserted=1 + len(plan.warnings),
                ),
            )
            conn.commit()
        except Exception:
            pass

        return result

    except Exception as exc:
        conn.rollback()

        # Try to log failure, but do not hide the original exception if logging
        # also fails.
        try:
            record_ingest_run(
                conn,
                IngestLogEntry(
                    source_id=plan.source_id,
                    data_source_type=SOURCE_TYPE,
                    action="build",
                    status="failed",
                    message=str(exc),
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
# Core resolution logic
# -----------------------------------------------------------------------------

def resolve_event_from_staging(
    conn: sqlite3.Connection,
    source_id: str,
    inventory_snapshot_id: int,
    staged: dict[str, StagedFireParameter],
    errors: list[dict[str, Any]],
) -> tuple[ResolvedFireEvent | None, list[FireEventWarning]]:
    """
    Resolve staged key/value rows into one model-facing fire event.

    This is the main transformation function.
    """
    warnings: list[FireEventWarning] = []

    # -------------------------------------------------
    # Fire spread category
    # -------------------------------------------------
    fire_spread_input = get_text(staged, PARAM_FIRE_SPREAD)

    if fire_spread_input is None:
        errors.append(error_record(
            "missing_required_input",
            fire_parameter=PARAM_FIRE_SPREAD,
        ))
        return None, warnings

    fire_spread_category = resolve_input_value_mapping(
        conn=conn,
        name_category=PARAM_FIRE_SPREAD,
        input_value=fire_spread_input,
        errors=errors,
    )

    if fire_spread_category is None:
        return None, warnings

    if fire_spread_category not in VALID_FIRE_SPREAD_CLASSES:
        errors.append(error_record(
            "invalid_resolved_fire_spread_category",
            fire_parameter=PARAM_FIRE_SPREAD,
            input_value=fire_spread_input,
            resolved_value=fire_spread_category,
            allowed=sorted(VALID_FIRE_SPREAD_CLASSES),
        ))
        return None, warnings

    # -------------------------------------------------
    # Room of origin
    # -------------------------------------------------
    room_of_origin_input = get_text(staged, PARAM_ROOM_ORIGIN)
    room_of_origin = None

    if room_of_origin_input is not None:
        room_of_origin = resolve_room_of_origin(
            conn=conn,
            inventory_snapshot_id=inventory_snapshot_id,
            room_description=room_of_origin_input,
            errors=errors,
        )

    elif fire_spread_category in ROOM_REQUIRED_SPREAD_CLASSES:
        errors.append(error_record(
            "missing_room_of_origin",
            fire_parameter=PARAM_ROOM_ORIGIN,
            fire_spread_category=fire_spread_category,
            detail="room_of_origin is required for room-scale and multiple-room fire scenarios.",
        ))

    # -------------------------------------------------
    # Numeric values
    # -------------------------------------------------
    fire_area_m2 = get_numeric(staged, PARAM_FIRE_AREA)
    smoke_heat_damage_area_m2 = get_numeric(staged, PARAM_SMOKE_HEAT_AREA)
    room_of_origin_size_m2 = get_numeric(staged, PARAM_ROOM_SIZE)
    dwelling_size_m2 = get_numeric(staged, PARAM_DWELLING_SIZE)

    # This should already be caught during raw ingest, but we re-check here
    # because this table is the model-facing gatekeeper.
    if fire_spread_category != "single_item":
        if fire_area_m2 is None or fire_area_m2 <= 0:
            errors.append(error_record(
                "missing_or_zero_fire_area_for_non_single_item",
                fire_parameter=PARAM_FIRE_AREA,
                fire_spread_category=fire_spread_category,
                detail=(
                    "fire_area_m2 must contain a non-zero value unless the fire "
                    "is confined to a single item."
                ),
            ))

    # Non-blocking warnings where later calculations can use defaults/fallbacks.
    if smoke_heat_damage_area_m2 is None:
        warnings.append(FireEventWarning(
            source_id=source_id,
            warning_type="missing_smoke_heat_damage_area_m2",
            fire_parameter=PARAM_SMOKE_HEAT_AREA,
            warning_message=(
                "Smoke/heat damage area is missing. Replacement item calculations "
                "will default to using combusted-item/fire-area data instead. "
                "Include smoke_heat_damage_area_m2 to improve accuracy."
            ),
            warning_severity="model_assumption",
        ))

    if room_of_origin_size_m2 is None:
        warnings.append(FireEventWarning(
            source_id=source_id,
            warning_type="missing_room_of_origin_size_m2",
            fire_parameter=PARAM_ROOM_SIZE,
            warning_message=(
                "Room of origin size is missing. The model will default to using "
                "the mean room size from the inventory snapshot, which may be "
                "significantly different from the actual room size."
            ),
            warning_severity="model_assumption",
        ))

    if dwelling_size_m2 is None:
        warnings.append(FireEventWarning(
            source_id=source_id,
            warning_type="missing_dwelling_size_m2",
            fire_parameter=PARAM_DWELLING_SIZE,
            warning_message=(
                "Dwelling size is missing. The model will default to using "
                "a mean dwelling size, which may be significantly different from "
                "the actual dwelling size."
            ),
            warning_severity="model_assumption",
        ))

    # Simple physical consistency checks where values are supplied.
    if room_of_origin_size_m2 is not None and dwelling_size_m2 is not None:
        if room_of_origin_size_m2 > dwelling_size_m2:
            errors.append(error_record(
                "room_size_exceeds_dwelling_size",
                room_of_origin_size_m2=room_of_origin_size_m2,
                dwelling_size_m2=dwelling_size_m2,
            ))

    if fire_area_m2 is not None and dwelling_size_m2 is not None:
        if fire_area_m2 > dwelling_size_m2:
            errors.append(error_record(
                "fire_area_exceeds_dwelling_size",
                fire_area_m2=fire_area_m2,
                dwelling_size_m2=dwelling_size_m2,
            ))

    if fire_spread_category == "within_room":
        if fire_area_m2 is not None and room_of_origin_size_m2 is not None:
            if fire_area_m2 > room_of_origin_size_m2:
                errors.append(error_record(
                    "within_room_fire_area_exceeds_room_size",
                    fire_area_m2=fire_area_m2,
                    room_of_origin_size_m2=room_of_origin_size_m2,
                ))

    if fire_spread_category == "multiple_rooms":
        if fire_area_m2 is not None and room_of_origin_size_m2 is not None:
            if fire_area_m2 <= room_of_origin_size_m2:
                errors.append(error_record(
                    "multiple_room_fire_area_not_beyond_origin_room",
                    fire_area_m2=fire_area_m2,
                    room_of_origin_size_m2=room_of_origin_size_m2,
                ))

    # -------------------------------------------------
    # Dwelling type
    # -------------------------------------------------
    dwelling_type_input = get_text(staged, PARAM_DWELLING_TYPE)
    dwelling_type = None

    if dwelling_type_input is not None:
        dwelling_type = resolve_input_value_mapping(
            conn=conn,
            name_category=PARAM_DWELLING_TYPE,
            input_value=dwelling_type_input,
            errors=errors,
        )

        if dwelling_type is not None:
            validate_dwelling_type(
                conn=conn,
                inventory_snapshot_id=inventory_snapshot_id,
                dwelling_type=dwelling_type,
                errors=errors,
            )

    # -------------------------------------------------
    # Ignition source / single-item mapping
    # -------------------------------------------------
    ignition_source_category = get_text(staged, PARAM_IGNITION_SOURCE_CATEGORY)
    ignition_source = get_text(staged, PARAM_IGNITION_SOURCE)

    single_item_status = None
    item_combusted = None

    if fire_spread_category == "single_item":
        if ignition_source is None:
            errors.append(error_record(
                "single_item_missing_ignition_source",
                fire_parameter=PARAM_IGNITION_SOURCE,
                detail="single_item fires require an ignition_source value.",
            ))

        else:
            ignition_mapping = resolve_ignition_item_mapping(
                conn=conn,
                ignition_source=ignition_source,
                ignition_source_category=ignition_source_category,
                errors=errors,
            )

            if ignition_mapping is not None:
                single_item_status = ignition_mapping["single_item_status"]
                item_combusted = ignition_mapping["item_combusted"]

                if single_item_status == "proxy_inventory_item":
                    warnings.append(FireEventWarning(
                        source_id=source_id,
                        warning_type="proxy_inventory_item_used",
                        fire_parameter=PARAM_IGNITION_SOURCE,
                        warning_message=(
                            "This exact ignition source does not exist in the inventory database, "
                            "so a similar proxy item will be used to calculate the emissions."
                        ),
                        warning_severity="model_assumption",
                    ))

                if single_item_status in {"invalid_single_item", "unmapped"}:
                    warnings.append(FireEventWarning(
                        source_id=source_id,
                        warning_type="default_single_item_value_required",
                        fire_parameter=PARAM_IGNITION_SOURCE,
                        warning_message=(
                            "This ignition source does not have an assigned carbon stock value "
                            "associated with it. The default single-item emission value will be "
                            "returned instead."
                        ),
                        warning_severity="model_assumption",
                    ))

                if item_combusted is not None:
                    validate_item_combusted(
                        conn=conn,
                        inventory_snapshot_id=inventory_snapshot_id,
                        item_combusted=item_combusted,
                        errors=errors,
                    )

    # If blocking errors have appeared, do not return a partially resolved event.
    if errors:
        return None, warnings

    # Preserve input notes inside resolution_notes for now.
    # If we later add a dedicated input_notes column, this can be separated.
    input_notes = get_text(staged, PARAM_INPUT_NOTES)
    resolution_notes = None

    if input_notes is not None:
        resolution_notes = f"Input notes: {input_notes}"

    event = ResolvedFireEvent(
        source_id=source_id,
        inventory_snapshot_id=inventory_snapshot_id,

        fire_spread_category_input=fire_spread_input,
        fire_spread_category=fire_spread_category,

        room_of_origin_input=room_of_origin_input,
        room_of_origin=room_of_origin,

        fire_area_m2=fire_area_m2,
        smoke_heat_damage_area_m2=smoke_heat_damage_area_m2,
        room_of_origin_size_m2=room_of_origin_size_m2,
        dwelling_size_m2=dwelling_size_m2,

        dwelling_type_input=dwelling_type_input,
        dwelling_type=dwelling_type,

        ignition_source=ignition_source,
        single_item_status=single_item_status,
        item_combusted=item_combusted,

        resolution_notes=resolution_notes,
    )

    return event, warnings


# -----------------------------------------------------------------------------
# Staging-table loading helpers
# -----------------------------------------------------------------------------

def get_single_staged_source_id(
    conn: sqlite3.Connection,
    errors: list[dict[str, Any]],
) -> str | None:
    """
    Return the single source_id currently present in input_single_event.

    Current design assumes:
        one staged input workbook = one source_id = one fire event.
    """
    rows = conn.execute(
        f"""
        SELECT DISTINCT source_id
        FROM {TABLE_SINGLE_EVENT_INPUT}
        WHERE source_id IS NOT NULL
        """
    ).fetchall()

    if len(rows) == 0:
        errors.append(error_record(
            "no_staged_fire_event_input",
            detail="No rows found in input_single_event. Run fire_event ingest first.",
        ))
        return None

    if len(rows) > 1:
        errors.append(error_record(
            "multiple_staged_source_ids",
            source_ids=[str(r["source_id"]) for r in rows],
            detail=(
                "Multiple source_id values were found in input_single_event. "
                "Current resolver expects one staged fire event at a time."
            ),
        ))
        return None

    return str(rows[0]["source_id"])


def load_staged_parameters(
    conn: sqlite3.Connection,
    source_id: str,
    errors: list[dict[str, Any]],
) -> dict[str, StagedFireParameter]:
    """
    Load staged fire input parameters for one source_id.

    Returns:
        dict keyed by fire_parameter.
    """
    rows = conn.execute(
        f"""
        SELECT
            fire_parameter,
            value_text,
            value_numeric
        FROM {TABLE_SINGLE_EVENT_INPUT}
        WHERE source_id = ?
        ORDER BY input_row
        """,
        (source_id,),
    ).fetchall()

    out: dict[str, StagedFireParameter] = {}

    for r in rows:
        fire_parameter = str(r["fire_parameter"])

        if fire_parameter in out:
            errors.append(error_record(
                "duplicate_staged_fire_parameter",
                source_id=source_id,
                fire_parameter=fire_parameter,
            ))
            continue

        out[fire_parameter] = StagedFireParameter(
            fire_parameter=fire_parameter,
            value_text=None if r["value_text"] is None else str(r["value_text"]),
            value_numeric=None if r["value_numeric"] is None else float(r["value_numeric"]),
        )

    return out


# -----------------------------------------------------------------------------
# Mapping / lookup helpers
# -----------------------------------------------------------------------------

def resolve_input_value_mapping(
    conn: sqlite3.Connection,
    name_category: str,
    input_value: str,
    errors: list[dict[str, Any]],
) -> str | None:
    """
    Resolve a user-facing input value to its canonical value.

    Used for:
        fire_spread_category
        dwelling_type
    """
    rows = conn.execute(
        f"""
        SELECT canonical_value
        FROM {TABLE_FIRE_INPUT_VALUE_MAPPING}
        WHERE name_category = ?
          AND input_value = ?
        """,
        (name_category, input_value),
    ).fetchall()

    if len(rows) == 0:
        errors.append(error_record(
            "unmapped_input_value",
            name_category=name_category,
            input_value=input_value,
        ))
        return None

    if len(rows) > 1:
        errors.append(error_record(
            "ambiguous_input_value_mapping",
            name_category=name_category,
            input_value=input_value,
            n_matches=len(rows),
        ))
        return None

    return str(rows[0]["canonical_value"])


def resolve_room_of_origin(
    conn: sqlite3.Connection,
    inventory_snapshot_id: int,
    room_description: str,
    errors: list[dict[str, Any]],
) -> str | None:
    """
    Resolve user-facing room_description to canonical room_type.
    """
    rows = conn.execute(
        f"""
        SELECT room_type
        FROM {TABLE_INVENTORY_ROOM_SNAPSHOT}
        WHERE inventory_snapshot_id = ?
          AND room_description = ?
        """,
        (inventory_snapshot_id, room_description),
    ).fetchall()

    if len(rows) == 0:
        errors.append(error_record(
            "room_of_origin_not_in_inventory_snapshot",
            room_description=room_description,
        ))
        return None

    if len(rows) > 1:
        errors.append(error_record(
            "ambiguous_room_of_origin_mapping",
            room_description=room_description,
            n_matches=len(rows),
        ))
        return None

    return str(rows[0]["room_type"])


def validate_dwelling_type(
    conn: sqlite3.Connection,
    inventory_snapshot_id: int,
    dwelling_type: str,
    errors: list[dict[str, Any]],
) -> None:
    """
    Check that resolved dwelling_type exists in the current inventory snapshot.
    """
    row = conn.execute(
        f"""
        SELECT 1
        FROM {TABLE_INVENTORY_DWELLING_SIZE_SNAPSHOT}
        WHERE inventory_snapshot_id = ?
          AND dwelling_type = ?
        LIMIT 1
        """,
        (inventory_snapshot_id, dwelling_type),
    ).fetchone()

    if row is None:
        errors.append(error_record(
            "dwelling_type_not_in_inventory_snapshot",
            dwelling_type=dwelling_type,
        ))


def resolve_ignition_item_mapping(
    conn: sqlite3.Connection,
    ignition_source: str,
    ignition_source_category: str | None,
    errors: list[dict[str, Any]],
) -> dict[str, Any] | None:
    """
    Resolve ignition source to single_item_status and optional item_combusted.

    Prefer category + source matching, because FRIS contains repeated labels
    such as "Other".

    If no category is available, fall back to source-only matching:
        - if there is exactly one match, use it
        - if there are multiple matches that all mean "not modelled", use one
        - otherwise return a blocking ambiguity error
    """
    if ignition_source_category is not None:
        rows = conn.execute(
            f"""
            SELECT
                single_item_status,
                item_combusted
            FROM {TABLE_FIRE_IGNITION_ITEM_MAPPING}
            WHERE ignition_source_category = ?
              AND ignition_source = ?
            """,
            (ignition_source_category, ignition_source),
        ).fetchall()

        if len(rows) == 1:
            return {
                "single_item_status": str(rows[0]["single_item_status"]),
                "item_combusted": rows[0]["item_combusted"],
            }

        if len(rows) > 1:
            errors.append(error_record(
                "ambiguous_ignition_source_mapping",
                ignition_source_category=ignition_source_category,
                ignition_source=ignition_source,
                n_matches=len(rows),
            ))
            return None

    # Fallback: source-only lookup.
    rows = conn.execute(
        f"""
        SELECT
            ignition_source_category,
            single_item_status,
            item_combusted
        FROM {TABLE_FIRE_IGNITION_ITEM_MAPPING}
        WHERE ignition_source = ?
        """,
        (ignition_source,),
    ).fetchall()

    if len(rows) == 0:
        errors.append(error_record(
            "ignition_source_not_in_mapping",
            ignition_source_category=ignition_source_category,
            ignition_source=ignition_source,
        ))
        return None

    if len(rows) == 1:
        return {
            "single_item_status": str(rows[0]["single_item_status"]),
            "item_combusted": rows[0]["item_combusted"],
        }

    # If duplicate source labels all mean "no item-specific calculation",
    # downstream behaviour is the same. This handles duplicate "Other" rows.
    all_unmodelled = all(
        str(r["single_item_status"]) in {"invalid_single_item", "unmapped"}
        and r["item_combusted"] is None
        for r in rows
    )

    if all_unmodelled:
        return {
            "single_item_status": str(rows[0]["single_item_status"]),
            "item_combusted": None,
        }

    errors.append(error_record(
        "ambiguous_ignition_source_mapping",
        ignition_source=ignition_source,
        n_matches=len(rows),
        detail=(
            "Multiple ignition-source mappings were found. Provide "
            "ignition_source_category or make the mapping unambiguous."
        ),
    ))
    return None


def validate_item_combusted(
    conn: sqlite3.Connection,
    inventory_snapshot_id: int,
    item_combusted: str,
    errors: list[dict[str, Any]],
) -> None:
    """
    Check that item_combusted exists in the current inventory item snapshot.
    """
    row = conn.execute(
        f"""
        SELECT 1
        FROM {TABLE_INVENTORY_ITEM_SNAPSHOT}
        WHERE inventory_snapshot_id = ?
          AND item_name = ?
        LIMIT 1
        """,
        (inventory_snapshot_id, item_combusted),
    ).fetchone()

    if row is None:
        errors.append(error_record(
            "item_combusted_not_in_inventory_snapshot",
            item_combusted=item_combusted,
        ))


def get_current_inventory_snapshot_id(
    conn: sqlite3.Connection,
    errors: list[dict[str, Any]],
) -> int | None:
    """
    Return the current inventory_snapshot_id.

    Current fire_db design keeps only the current snapshot.
    """
    rows = conn.execute(
        f"""
        SELECT inventory_snapshot_id
        FROM {TABLE_INVENTORY_SNAPSHOT}
        ORDER BY inventory_snapshot_id DESC
        """
    ).fetchall()

    if len(rows) == 0:
        errors.append(error_record(
            "missing_inventory_snapshot",
            detail="Run scripts.fire.inventory_snapshot before building fire_events.",
        ))
        return None

    if len(rows) > 1:
        errors.append(error_record(
            "multiple_inventory_snapshots",
            n_snapshots=len(rows),
            detail="fire_db is expected to contain only the current inventory snapshot.",
        ))
        return None

    return int(rows[0]["inventory_snapshot_id"])


# -----------------------------------------------------------------------------
# Insert helpers
# -----------------------------------------------------------------------------

def insert_fire_event_input(
    conn: sqlite3.Connection,
    event: ResolvedFireEvent,
) -> None:
    """
    Insert one resolved model-facing fire event row.
    """
    conn.execute(
        f"""
        INSERT INTO {TABLE_FIRE_EVENTS} (
            source_id,
            inventory_snapshot_id,

            fire_spread_category_input,
            fire_spread_category,

            room_of_origin_input,
            room_of_origin,

            fire_area_m2,
            smoke_heat_damage_area_m2,
            room_of_origin_size_m2,
            dwelling_size_m2,

            dwelling_type_input,
            dwelling_type,

            ignition_source,
            single_item_status,
            item_combusted,

            resolution_notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event.source_id,
            event.inventory_snapshot_id,

            event.fire_spread_category_input,
            event.fire_spread_category,

            event.room_of_origin_input,
            event.room_of_origin,

            event.fire_area_m2,
            event.smoke_heat_damage_area_m2,
            event.room_of_origin_size_m2,
            event.dwelling_size_m2,

            event.dwelling_type_input,
            event.dwelling_type,

            event.ignition_source,
            event.single_item_status,
            event.item_combusted,

            event.resolution_notes,
        ),
    )


def insert_fire_event_warnings(
    conn: sqlite3.Connection,
    warnings: list[FireEventWarning],
) -> None:
    """
    Insert structured warning rows for one resolved event.
    """
    if not warnings:
        return

    payload = [
        (
            w.source_id,
            w.warning_type,
            w.warning_severity,
            w.fire_parameter,
            w.warning_message,
        )
        for w in warnings
    ]

    conn.executemany(
        f"""
        INSERT INTO {TABLE_FIRE_EVENT_WARNINGS} (
            source_id,
            warning_type,
            warning_severity,
            fire_parameter,
            warning_message
        ) VALUES (?, ?, ?, ?, ?)
        """,
        payload,
    )


# -----------------------------------------------------------------------------
# Validation / DB helpers
# -----------------------------------------------------------------------------

def validate_required_schema(
    conn: sqlite3.Connection,
    errors: list[dict[str, Any]],
) -> None:
    """
    Check that all required fire_db tables exist.
    """
    existing = list_tables(conn)

    required = {
        TABLE_SOURCES,
        TABLE_INGEST_LOG,

        TABLE_SINGLE_EVENT_INPUT,
        TABLE_FIRE_INPUT_VALUE_MAPPING,
        TABLE_FIRE_IGNITION_ITEM_MAPPING,

        TABLE_INVENTORY_SNAPSHOT,
        TABLE_INVENTORY_ROOM_SNAPSHOT,
        TABLE_INVENTORY_ITEM_SNAPSHOT,
        TABLE_INVENTORY_DWELLING_SIZE_SNAPSHOT,

        TABLE_FIRE_EVENTS,
        TABLE_FIRE_EVENT_WARNINGS,
    }

    missing = sorted(required - existing)

    for table in missing:
        errors.append(error_record(
            "missing_required_table",
            table=table,
            detail="Run or update scripts.fire.init_fire_db first.",
        ))


def list_tables(conn: sqlite3.Connection) -> set[str]:
    """
    Return all table names in the current database.
    """
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        """
    ).fetchall()

    return {str(r["name"]) for r in rows}


def count_rows(conn: sqlite3.Connection, table_name: str) -> int:
    """
    Count rows in a database table.
    """
    row = conn.execute(f"SELECT COUNT(*) AS n FROM {table_name}").fetchone()
    return int(row["n"])



def count_fire_events_rows_for_source(
    conn: sqlite3.Connection,
    source_id: str,
) -> int:
    """
    Count promoted/model-facing fire event rows for one source_id.

    Current design:
        source_id is used as the event identifier in fire_events.

    Expected behaviour:
        Before build:
            0 rows

        After successful build:
            1 row

    If this returns > 1, something has gone wrong with table constraints,
    because source_id should be the PRIMARY KEY in fire_events.
    """
    row = conn.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM {TABLE_FIRE_EVENTS}
        WHERE source_id = ?
        """,
        (source_id,),
    ).fetchone()

    return int(row["n"])


def fire_event_exists(
    conn: sqlite3.Connection,
    source_id: str,
) -> bool:
    """
    Return True if the current staged source_id has already been promoted into
    fire_events.

    This prevents accidental duplicate insertion when the user runs:

        python -m scripts.fire.build_fire_event_input --profile tom --db fire_db --apply

    multiple times after the same ingest.
    """
    return count_fire_events_rows_for_source(
        conn=conn,
        source_id=source_id,
    ) > 0


def get_text(
    staged: dict[str, StagedFireParameter],
    fire_parameter: str,
) -> str | None:
    """
    Return a staged text value.
    """
    row = staged.get(fire_parameter)

    if row is None:
        return None

    return row.value_text


def get_numeric(
    staged: dict[str, StagedFireParameter],
    fire_parameter: str,
) -> float | None:
    """
    Return a staged numeric value.
    """
    row = staged.get(fire_parameter)

    if row is None:
        return None

    return row.value_numeric


def error_record(error_type: str, **kwargs: Any) -> dict[str, Any]:
    """
    Build a structured error dictionary for reporting/debugging.
    """
    out = {"type": error_type}
    out.update(kwargs)
    return out


# -----------------------------------------------------------------------------
# Reporting helpers
# -----------------------------------------------------------------------------

def print_plan(plan: ResolvePlan) -> None:
    """
    Print dry-run / preview details.
    """
    print("\nFire event input build preview")
    print("------------------------------")

    print(f"Existing fire_events rows:         {plan.existing_event_rows}")
    print(f"Existing fire_events_warning rows: {plan.existing_warning_rows}")

    if plan.source_id is not None:
        print(f"\nStaged source_id: {plan.source_id}")

    if plan.inventory_snapshot_id is not None:
        print(f"Inventory snapshot ID: {plan.inventory_snapshot_id}")

    if plan.resolved_event is not None:
        event = plan.resolved_event

        print("\nResolved event:")
        print(f"  fire_spread_category_input: {event.fire_spread_category_input}")
        print(f"  fire_spread_category:       {event.fire_spread_category}")
        print(f"  room_of_origin_input:       {event.room_of_origin_input}")
        print(f"  room_of_origin:             {event.room_of_origin}")
        print(f"  fire_area_m2:               {event.fire_area_m2}")
        print(f"  smoke_heat_damage_area_m2:  {event.smoke_heat_damage_area_m2}")
        print(f"  room_of_origin_size_m2:     {event.room_of_origin_size_m2}")
        print(f"  dwelling_size_m2:           {event.dwelling_size_m2}")
        print(f"  dwelling_type_input:        {event.dwelling_type_input}")
        print(f"  dwelling_type:              {event.dwelling_type}")
        print(f"  ignition_source:            {event.ignition_source}")
        print(f"  single_item_status:         {event.single_item_status}")
        print(f"  item_combusted:             {event.item_combusted}")

    if plan.warnings:
        print("\nWarnings that would be inserted:")
        for warning in plan.warnings:
            print(f"  - {warning.warning_type}: {warning.warning_message}")

    if plan.errors:
        print("\nBlocking errors:")
        for error in plan.errors:
            print(f"  - {error}")

    if not plan.errors:
        print("\nDry run complete. Re-run with --apply to build fire_events.")


def print_result(result: ResolveResult) -> None:
    """
    Print apply summary.
    """
    print("\nFire event input built")
    print("----------------------")
    print(f"source_id:              {result.source_id}")
    print(f"inventory_snapshot_id:  {result.inventory_snapshot_id}")
    print(f"event rows inserted:    {result.rows_inserted_event}")
    print(f"warning rows inserted:  {result.rows_inserted_warnings}")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="build_fire_event_input",
        description=(
            "Resolve staged fire input parameters into the model-facing "
            "fire_events table. Dry-run is default; use --apply to write."
        ),
    )

    parser.add_argument(
        "--profile",
        required=True,
        help="Profile name from config/local_paths.yaml, e.g. tom.",
    )

    parser.add_argument(
        "--db",
        required=True,
        help="Database handle from config/local_paths.yaml, e.g. fire_db or test_db.",
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the build. Without this flag, only a dry-run preview is shown.",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Delete existing fire_events and fire_event_warnings before inserting "
            "the current staged event. Without this flag, existing model-facing "
            "records are kept and the current event is appended if new."
        ),
    )

    args = parser.parse_args(argv)

    config = load_local_paths_config(Path("config") / "local_paths.yaml")

    resolved = resolve_db_path(
        profile=args.profile,
        db_handle=args.db,
        config=config,
    )

    result = build_fire_event_input(
        db_path=resolved.db_path,
        apply=args.apply,
        overwrite=args.overwrite,
    )

    if isinstance(result, ResolvePlan):
        print_plan(result, overwrite=args.overwrite)
        return 1 if result.has_blocking_errors else 0

    print_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())