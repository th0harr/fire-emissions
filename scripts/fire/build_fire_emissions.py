from __future__ import annotations

"""
Build deterministic Fire Emissions model outputs.

Current scope
-------------
This first implementation builds Stage 1 only:
    - fire_model_metadata
    - fire_model_stage1_component_results
    - fire_model_warnings

It deliberately does not yet populate:
    - fire_model_stage2_species_results

Stage 2 will be added after Stage 1 has been checked against the current
fire_events and inventory snapshot data.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.fire import fire_model_io
from scripts.fire.fire_stock_model import build_stage1_component_results
from scripts.fire.fire_species_model import build_stage2_species_results


# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------

MODEL_NAME = "fire_emissions"
MODEL_VERSION = "stage2_v0.1"


# -----------------------------------------------------------------------------
# SMALL HELPERS
# -----------------------------------------------------------------------------

def utc_now_iso() -> str:
    """
    Return a compact UTC timestamp for database metadata.
    """
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


# -----------------------------------------------------------------------------
# PUBLIC BUILD FUNCTION
# -----------------------------------------------------------------------------

def build_fire_emissions(
    db_path: str | Path,
    *,
    input_type: str = "fris",
    overwrite: bool = True,
) -> dict[str, Any]:
    """
    Build the current deterministic fire-emissions model outputs.

    Parameters
    ----------
    db_path:
        Path to the SQLite fire_db database.

    input_type:
        Fire-event route to model.  The current main route is 'fris'.

    overwrite:
        If True, clear current fire_model_* output tables before writing.
        This is the intended first-pass behaviour because we are not yet keeping
        model-run history.

    Returns
    -------
    dict
        Summary dictionary for scripts/model.py to print.
    """
    db_path = Path(db_path)

    started_utc = utc_now_iso()
    created_at_utc = started_utc

    conn = fire_model_io.db_connect(db_path)

    try:
        conn.execute("PRAGMA foreign_keys = ON;")

        # ---- Validate schema before doing any destructive work ----
        fire_model_io.validate_fire_model_schema(conn)

        # ---- Load input data ----
        events = fire_model_io.load_fire_events(conn, input_type=input_type)

        upstream_omission_summary = fire_model_io.load_fire_event_omission_summary(
            conn,
            input_type=input_type,
        )

        event_rows_omitted_upstream = sum(
            int(row.get("omitted_count") or 0)
            for row in upstream_omission_summary
        )

        inventory_snapshot_id = fire_model_io.get_current_inventory_snapshot_id(conn)

        room_lookup = fire_model_io.load_room_stock_lookup(
            conn,
            inventory_snapshot_id=inventory_snapshot_id,
        )

        item_lookup = fire_model_io.load_item_stock_lookup(
            conn,
            inventory_snapshot_id=inventory_snapshot_id,
        )

        dwelling_size_lookup = fire_model_io.load_dwelling_size_lookup(
            conn,
            inventory_snapshot_id=inventory_snapshot_id,
        )

        emission_parameters = fire_model_io.load_emission_parameters(conn)

        whole_dwelling_carbon = fire_model_io.build_whole_dwelling_carbon(room_lookup)
        whole_dwelling_embodied = fire_model_io.build_whole_dwelling_embodied(room_lookup)

        # ---- Build Stage 1 rows in memory first ----
        # This means that if the model fails halfway through, 
        # we have not yet deleted or written any output tables.
        stage1_rows, warnings = build_stage1_component_results(
            events=events,
            inventory_snapshot_id=inventory_snapshot_id,
            room_lookup=room_lookup,
            item_lookup=item_lookup,
            dwelling_size_lookup=dwelling_size_lookup,
            emission_parameters=emission_parameters,
            whole_dwelling_carbon=whole_dwelling_carbon,
            whole_dwelling_embodied=whole_dwelling_embodied,
            created_at_utc=created_at_utc,
        )

        # ---- Count event coverage ----
        event_rows_read = len(events)

        # In the current FRIS route, normal omissions should already have happened
        # upstream, before fire_events was populated.
        #
        # This count is therefore a defensive model-stage count.  It should normally be
        # zero.  If it is non-zero, it means something reached fire_events even though
        # it was marked omit_from_model='yes'.
        event_rows_omitted_model_stage = sum(
            1
            for event in events
            if str(event.get("omit_from_model", "no")).lower() == "yes"
        )

        modelled_incidents = {
            row.incident_id
            for row in stage1_rows
            if row.incident_id is not None
        }

        # ---- Apply database writes ----
        conn.execute("BEGIN;")

        if overwrite:
            fire_model_io.clear_fire_model_outputs(conn)

        # Stage 1 is written first because Stage 2 is derived from Stage 1 direct rows.
        rows_stage1_written = fire_model_io.insert_stage1_results(conn, stage1_rows)

        # Load the just-written Stage 1 direct rows from the database.
        #
        # This gives Stage 2 access to any database-generated primary key such as
        # stage1_result_id, if that column exists in the current schema.
        stage1_direct_rows = fire_model_io.load_stage1_direct_results(
            conn,
            input_type=input_type,
        )

        # Build Stage 2 in memory before inserting it.
        #
        # If Stage 2 fails, the surrounding transaction will roll back the Stage 1
        # insert as well, keeping model outputs consistent.
        stage2_rows, stage2_warnings = build_stage2_species_results(
            stage1_direct_rows=stage1_direct_rows,
            emission_parameters=emission_parameters,
            created_at_utc=created_at_utc,
        )

        warnings.extend(stage2_warnings)

        rows_stage2_written = fire_model_io.insert_stage2_results(conn, stage2_rows)
        rows_warnings_written = fire_model_io.insert_model_warnings(conn, warnings)

        fire_model_io.upsert_model_omission_summary(
            conn,
            model_name=MODEL_NAME,
            model_version=MODEL_VERSION,
            input_type=input_type,
            omit_reason="model_stage_omission",
            omitted_count=event_rows_omitted_model_stage,
        )
        
        finished_utc = utc_now_iso()

        fire_model_io.insert_model_metadata(
            conn,
            {
                "metadata_id": 1,
                "model_name": MODEL_NAME,
                "model_version": MODEL_VERSION,
                "model_description": (
                    "Deterministic Fire Emissions model. Current build populates "
                    "Stage 1 affected-stock / replacement embodied CO2 outputs and "
                    "Stage 2 combustion-species outputs for direct carbon rows."
                ),
                "input_type": input_type,
                "event_rows_read": event_rows_read,
                "event_rows_modelled": len(modelled_incidents),

                # Keep the old conceptual distinction clear:
                #   upstream omissions = omitted before fire_events
                #   model-stage omissions = reached fire_events but skipped by model
                "event_rows_omitted_upstream": event_rows_omitted_upstream,
                "event_rows_omitted_model_stage": event_rows_omitted_model_stage,
                "event_rows_total_before_upstream_omissions": (
                    event_rows_read + event_rows_omitted_upstream
                ),

                "emission_parameter_source_id": _latest_emission_parameter_source_id(conn),
                "emission_parameter_rows": _count_emission_parameter_rows(conn),
                "inventory_snapshot_id": inventory_snapshot_id,
                "estimate_cases_built": "low;default;high",
                "include_area_range": 1,
                "include_stock_range": 1,
                "include_emission_parameter_range": 1,
                "started_utc": started_utc,
                "finished_utc": finished_utc,
                "created_at_utc": created_at_utc,
                "rows_stage1_written": rows_stage1_written,
                "rows_stage2_written": rows_stage2_written,
                "rows_warnings_written": rows_warnings_written,
                "notes": (
                    "Stage 2 converts Stage 1 direct affected-carbon rows to CO2 and CO. "
                    "Replacement embodied CO2 remains a separate Stage 1/reporting stream."
                ),
            },
        )

        conn.commit()

        return {
            "model_name": MODEL_NAME,
            "model_version": MODEL_VERSION,
            "input_type": input_type,
            "overwrite": overwrite,
            "event_rows_read": event_rows_read,
            "event_rows_modelled": len(modelled_incidents),
            "event_rows_omitted_upstream": event_rows_omitted_upstream,
            "event_rows_omitted_model_stage": event_rows_omitted_model_stage,
            "event_rows_total_before_upstream_omissions": (
                event_rows_read + event_rows_omitted_upstream
            ),
            "room_stock_rows_loaded": len(room_lookup),
            "item_stock_rows_loaded": len(item_lookup),
            "stage1_direct_rows_loaded_for_stage2": len(stage1_direct_rows),
            "stage1_rows_written": rows_stage1_written,
            "stage2_rows_written": rows_stage2_written,
            "warnings_written": rows_warnings_written,
            "inventory_snapshot_id": inventory_snapshot_id,
        }

        finished_utc = utc_now_iso()

        fire_model_io.insert_model_metadata(
            conn,
            {
                "metadata_id": 1,
                "model_name": MODEL_NAME,
                "model_version": MODEL_VERSION,
                "model_description": (
                    "Deterministic Fire Emissions model. Current build populates "
                    "Stage 1 affected-stock and replacement embodied CO2 outputs."
                ),
                "input_type": input_type,
                "event_rows_read": event_rows_read,
                "event_rows_modelled": len(modelled_incidents),
                "event_rows_omitted_model_stage": event_rows_omitted_model_stage,
                "event_rows_omitted_upstream": event_rows_omitted_upstream,
                "event_rows_total_before_upstream_omissions": (
                    event_rows_read + event_rows_omitted_upstream
                ),
                "emission_parameter_source_id": _latest_emission_parameter_source_id(conn),
                "emission_parameter_rows": _count_emission_parameter_rows(conn),
                "inventory_snapshot_id": inventory_snapshot_id,
                "estimate_cases_built": "low;default;high",
                "include_area_range": 1,
                "include_stock_range": 1,
                "include_emission_parameter_range": 1,
                "started_utc": started_utc,
                "finished_utc": finished_utc,
                "created_at_utc": created_at_utc,
                "rows_stage1_written": rows_stage1_written,
                "rows_stage2_written": 0,
                "rows_warnings_written": rows_warnings_written,
                "notes": "Stage 2 species emissions not yet built in this implementation batch.",
            },
        )

        conn.commit()

        return {
            "model_name": MODEL_NAME,
            "model_version": MODEL_VERSION,
            "input_type": input_type,
            "overwrite": overwrite,
            "event_rows_read": event_rows_read,
            "event_rows_modelled": len(modelled_incidents),
            "event_rows_omitted_model_stage": event_rows_omitted_model_stage,
            "event_rows_omitted_upstream": event_rows_omitted_upstream,
            "event_rows_total_before_upstream_omissions": (
                event_rows_read + event_rows_omitted_upstream
            ),
            "room_stock_rows_loaded": len(room_lookup),
            "item_stock_rows_loaded": len(item_lookup),
            "stage1_rows_written": rows_stage1_written,
            "stage2_rows_written": 0,
            "warnings_written": rows_warnings_written,
            "inventory_snapshot_id": inventory_snapshot_id,
        }

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise

    finally:
        conn.close()


# -----------------------------------------------------------------------------
# METADATA HELPERS
# -----------------------------------------------------------------------------

def _latest_emission_parameter_source_id(conn) -> str | None:
    """
    Return the source_id attached to the latest emission-parameter rows.
    """
    if "fire_emission_parameter_mapping" not in fire_model_io.list_tables(conn):
        return None

    row = conn.execute(
        """
        SELECT source_id
        FROM fire_emission_parameter_mapping
        ORDER BY created_at_utc DESC
        LIMIT 1;
        """
    ).fetchone()

    if row is None:
        return None

    return row[0]


def _count_emission_parameter_rows(conn) -> int:
    """
    Count current emission parameter rows.
    """
    if "fire_emission_parameter_mapping" not in fire_model_io.list_tables(conn):
        return 0

    row = conn.execute("SELECT COUNT(*) FROM fire_emission_parameter_mapping;").fetchone()
    return int(row[0]) if row is not None else 0
