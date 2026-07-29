from __future__ import annotations

"""
SQLite read/write helpers for the deterministic Fire Emissions model.

This module is deliberately focused on database input/output.  The calculation
logic lives in fire_area_model.py and fire_stock_model.py.

Important terminology
---------------------
SQLite is the database engine we are using.  SQL is the query language used to
read and write data in SQLite.  This module therefore contains Python code that
runs SQL statements against the existing SQLite fire_db file.
"""

import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Any, Optional

from scripts.fire.fire_model_records import (
    CarbonStockEstimate,
    EmbodiedCO2Estimate,
    FireModelWarning,
    ItemStockRecord,
    RoomStockRecord,
    Stage1ComponentResult,
    Stage2SpeciesResult,
)

# -----------------------------------------------------------------------------
# TABLE NAMES
# -----------------------------------------------------------------------------

TABLE_FIRE_EVENTS = "fire_events"
TABLE_FIRE_EVENT_WARNINGS = "fire_event_warnings"
TABLE_FIRE_EVENT_OMISSION_SUMMARY = "fire_event_omission_summary"
TABLE_FIRE_EMISSION_PARAMS = "fire_emission_parameter_mapping"

TABLE_INVENTORY_SNAPSHOT = "inventory_snapshot"
TABLE_INVENTORY_ROOM_SNAPSHOT = "inventory_room_snapshot"
TABLE_INVENTORY_DWELLING_SIZE_SNAPSHOT = "inventory_dwelling_size_snapshot"
TABLE_ITEM_CARBON_LOOKUP_VIEW = "v_inventory_item_carbon_lookup"
TABLE_AREA_BANDS = "fire_event_mapping_area_bands"

TABLE_MODEL_METADATA = "fire_model_metadata"
TABLE_STAGE1_RESULTS = "fire_model_stage1_component_results"
TABLE_STAGE2_RESULTS = "fire_model_stage2_species_results"
TABLE_MODEL_WARNINGS = "fire_model_warnings"



# -----------------------------------------------------------------------------
# BASIC SQLITE HELPERS
# -----------------------------------------------------------------------------

def db_connect(db_path: str | Path) -> sqlite3.Connection:
    """
    Open the exact SQLite database file supplied by the dispatcher.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def quote_ident(name: str) -> str:
    """
    Quote a SQLite table or column name.
    """
    return '"' + name.replace('"', '""') + '"'


def list_tables(conn: sqlite3.Connection) -> set[str]:
    """
    Return the set of table/view names in the current SQLite database.
    """
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type IN ('table', 'view');
        """
    ).fetchall()
    return {str(row[0]) for row in rows}


def table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    """
    Return column names for one table or view.
    """
    rows = conn.execute(f"PRAGMA table_info({quote_ident(table)});").fetchall()
    return [str(row[1]) for row in rows]


def get_existing_column(
    columns: list[str],
    aliases: list[str],
) -> Optional[str]:
    """
    Return the first matching column name from a list of possible aliases.

    This is useful because some inventory columns are still being added during
    development, especially the room-level embodied CO2 range columns.
    """
    lower_to_actual = {col.lower(): col for col in columns}

    for alias in aliases:
        found = lower_to_actual.get(alias.lower())
        if found is not None:
            return found

    return None


def row_get(row: sqlite3.Row | dict[str, Any], key: str, default: Any = None) -> Any:
    """
    Read a value from a SQLite row or dictionary using a normal key.
    """
    data = dict(row)
    return data.get(key, default)


def insert_dict_adaptive(
    conn: sqlite3.Connection,
    *,
    table: str,
    values: dict[str, Any],
) -> None:
    """
    Insert a dictionary into a table, ignoring fields that do not exist.

    This mirrors the adaptive insert pattern already used by the fire-event
    resolver.  It makes development easier while the schema is still moving.
    """
    cols = table_columns(conn, table)
    write_values = {key: value for key, value in values.items() if key in cols}

    if not write_values:
        raise RuntimeError(f"No matching columns to insert into {table}.")

    col_sql = ", ".join(quote_ident(col) for col in write_values.keys())
    q_sql = ", ".join("?" for _ in write_values)

    conn.execute(
        f"INSERT INTO {quote_ident(table)} ({col_sql}) VALUES ({q_sql});",
        list(write_values.values()),
    )


# -----------------------------------------------------------------------------
# SCHEMA VALIDATION / OUTPUT RESET
# -----------------------------------------------------------------------------

def validate_fire_model_schema(conn: sqlite3.Connection) -> None:
    """
    Check that the tables needed for Stage 1 modelling exist.
    """
    required = {
        TABLE_FIRE_EVENTS,
        TABLE_FIRE_EVENT_OMISSION_SUMMARY,
        TABLE_INVENTORY_SNAPSHOT,
        TABLE_INVENTORY_ROOM_SNAPSHOT,
        TABLE_INVENTORY_DWELLING_SIZE_SNAPSHOT,
        TABLE_ITEM_CARBON_LOOKUP_VIEW,
        TABLE_MODEL_METADATA,
        TABLE_STAGE1_RESULTS,
        TABLE_STAGE2_RESULTS,
        TABLE_MODEL_WARNINGS,
    }

    existing = list_tables(conn)
    missing = sorted(required - existing)

    if missing:
        raise RuntimeError(
            "fire_db is missing required fire-model table(s)/view(s): "
            + ", ".join(missing)
            + ". Run scripts.fire.init_fire_db and refresh inventory snapshots first."
        )


def clear_fire_model_outputs(conn: sqlite3.Connection) -> None:
    """
    Clear the current fire-model output tables before rebuilding.

    Stage 2 is deleted before Stage 1 because Stage 2 can reference Stage 1.
    """
    existing = list_tables(conn)

    for table in [
        TABLE_MODEL_WARNINGS,
        TABLE_STAGE2_RESULTS,
        TABLE_STAGE1_RESULTS,
        TABLE_MODEL_METADATA,
    ]:
        if table in existing:
            conn.execute(f"DELETE FROM {quote_ident(table)};")


# -----------------------------------------------------------------------------
# INPUT LOADERS
# -----------------------------------------------------------------------------

def get_current_inventory_snapshot_id(conn: sqlite3.Connection) -> int:
    """
    Return the current inventory snapshot id.

    The fire_db design currently expects one current inventory snapshot.  If
    more than one exists, the newest id is used, but a warning can be written by
    the calling module if desired.
    """
    row = conn.execute(
        f"""
        SELECT inventory_snapshot_id
        FROM {quote_ident(TABLE_INVENTORY_SNAPSHOT)}
        ORDER BY inventory_snapshot_id DESC
        LIMIT 1;
        """
    ).fetchone()

    if row is None:
        raise RuntimeError(
            "No inventory snapshot is available in fire_db. "
            "Run scripts.fire.inventory_snapshot first."
        )

    return int(row[0])


def load_fire_events(
    conn: sqlite3.Connection,
    *,
    input_type: str = "fris",
) -> list[dict[str, Any]]:
    """
    Load model-facing fire events for one input route.

    Omitted events are kept in the returned list so the builder can report how
    many were skipped.  Stage 1 will only model rows with omit_from_model = 'no'.
    """
    cols = table_columns(conn, TABLE_FIRE_EVENTS)

    if "input_type" in cols:
        rows = conn.execute(
            f"""
            SELECT *
            FROM {quote_ident(TABLE_FIRE_EVENTS)}
            WHERE input_type = ?
            ORDER BY incident_id;
            """,
            (input_type,),
        ).fetchall()
    else:
        rows = conn.execute(
            f"""
            SELECT *
            FROM {quote_ident(TABLE_FIRE_EVENTS)}
            ORDER BY incident_id;
            """
        ).fetchall()

    return [dict(row) for row in rows]


def load_stage1_direct_results(
    conn: sqlite3.Connection,
    *,
    input_type: str = "fris",
) -> list[dict[str, Any]]:
    """
    Load Stage 1 rows that should be converted by Stage 2.

    Stage 2 should only process direct affected-carbon rows.  It should not
    process replacement embodied CO2 rows.

    Selection logic:
        - input_type matches the requested route, where the column exists
        - emission_pathway = 'direct'
        - direct_total_kgC > 0
        - calculation_status is either missing, blank, or 'ok'

    Returns
    -------
    list[dict[str, Any]]
        Stage 1 rows as dictionaries, including stage1_result_id if the current
        schema has that column.
    """
    cols = table_columns(conn, TABLE_STAGE1_RESULTS)

    where_parts = [
        "LOWER(COALESCE(emission_pathway, '')) = 'direct'",
        "COALESCE(direct_total_kgC, 0) > 0",
    ]

    params: list[Any] = []

    if "input_type" in cols:
        where_parts.append("input_type = ?")
        params.append(input_type)

    if "calculation_status" in cols:
        where_parts.append("LOWER(COALESCE(calculation_status, 'ok')) = 'ok'")

    where_sql = " AND ".join(where_parts)

    rows = conn.execute(
        f"""
        SELECT *
        FROM {quote_ident(TABLE_STAGE1_RESULTS)}
        WHERE {where_sql}
        ORDER BY
            incident_id,
            estimate_case,
            component_type;
        """,
        params,
    ).fetchall()

    return [dict(row) for row in rows]


def load_fire_event_omission_summary(
    conn: sqlite3.Connection,
    *,
    input_type: str = "fris",
) -> list[dict[str, Any]]:
    """
    Load the latest upstream fire-event omission summary for one input route.

    These rows describe incidents omitted during build_fire_events before they
    reached the model-facing fire_events table.

    Important interpretation:
        Omitted incidents are not treated as zero-emission incidents.
        They are excluded from the current emissions estimate because the
        fire-event resolver could not create the required model-facing
        assumptions for those rows.

    The Stage 1 emissions model should not calculate anything from these rows.
    They are loaded only so the model metadata / later reporting layer can
    explain how many incidents were excluded upstream.
    """
    existing = list_tables(conn)

    if TABLE_FIRE_EVENT_OMISSION_SUMMARY not in existing:
        return []

    rows = conn.execute(
        f"""
        SELECT
            input_type,
            omit_reason,
            omitted_count,
            created_at_utc
        FROM {quote_ident(TABLE_FIRE_EVENT_OMISSION_SUMMARY)}
        WHERE input_type = ?
        ORDER BY omitted_count DESC, omit_reason;
        """,
        (input_type,),
    ).fetchall()

    return [dict(row) for row in rows]


def load_area_band_rows(conn: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    """
    Load area-band mapping rows.

    The Stage 1 model mostly parses the text label itself, but these rows are
    useful for QA and for future display ordering.
    """
    existing = list_tables(conn)
    if TABLE_AREA_BANDS not in existing:
        return {}

    rows = conn.execute(
        f"SELECT * FROM {quote_ident(TABLE_AREA_BANDS)} ORDER BY band_order;"
    ).fetchall()

    return {str(row["area_band"]): dict(row) for row in rows}


def load_emission_parameters(conn: sqlite3.Connection) -> dict[str, dict[str, dict[str, Optional[float]]]]:
    """
    Load emission parameters into a nested dictionary.

    Returned shape:
        params[fire_spread_category][fire_emission_parameter][estimate_case]

    For Stage 1, we currently need only additional_replacement_factor.  The
    same loader can later be reused for Stage 2 species parameters.
    """
    existing = list_tables(conn)
    if TABLE_FIRE_EMISSION_PARAMS not in existing:
        return {}

    rows = conn.execute(
        f"""
        SELECT
            fire_spread_category,
            fire_emission_parameter,
            is_applicable,
            value_min,
            value_default,
            value_max
        FROM {quote_ident(TABLE_FIRE_EMISSION_PARAMS)}
        WHERE is_applicable = 1;
        """
    ).fetchall()

    out: dict[str, dict[str, dict[str, Optional[float]]]] = {}

    for row in rows:
        category = str(row["fire_spread_category"])
        parameter = str(row["fire_emission_parameter"])

        out.setdefault(category, {})[parameter] = {
            "low": row["value_min"],
            "default": row["value_default"],
            "high": row["value_max"],
        }

    return out


def get_parameter_value(
    params: dict[str, dict[str, dict[str, Optional[float]]]],
    *,
    fire_spread_category: str,
    parameter_name: str,
    estimate_case: str,
    default: Optional[float] = None,
) -> Optional[float]:
    """
    Read one parameter value for one category and estimate case.

    If low/high are blank in the workbook, this falls back to the default value.
    """
    category_params = params.get(fire_spread_category, {})
    parameter = category_params.get(parameter_name)

    if parameter is None:
        return default

    value = parameter.get(estimate_case)
    if value is None:
        value = parameter.get("default")

    if value is None:
        return default

    return float(value)


def load_room_stock_lookup(
    conn: sqlite3.Connection,
    *,
    inventory_snapshot_id: int,
) -> dict[str, RoomStockRecord]:
    """
    Load room carbon stock and embodied CO2 values.

    The carbon-stock columns are fixed by the existing inventory snapshot.
    The embodied CO2 columns were added later, so this loader uses aliases to
    find them without requiring one exact name during development.
    """
    cols = table_columns(conn, TABLE_INVENTORY_ROOM_SNAPSHOT)

    # Column aliases for embodied CO2.  Add extra aliases here if the final
    # column names differ slightly from these guesses.
    embodied_default_col = get_existing_column(cols, [
        "expected_embodied_CO2_kg",
        "expected_embodied_co2_kg",
        "expected_room_embodied_CO2_kg",
        "expected_room_embodied_co2_kg",
        "embodied_CO2_kg",
        "embodied_co2_kg",
        "room_embodied_CO2_kg",
        "room_embodied_co2_kg",
    ])

    embodied_low_col = get_existing_column(cols, [
        "q25_embodied_CO2_kg",
        "q25_embodied_co2_kg",
        "q25_room_embodied_CO2_kg",
        "q25_room_embodied_co2_kg",
        "embodied_CO2_kg_q25",
        "embodied_co2_kg_q25",
        "room_embodied_CO2_kg_q25",
        "room_embodied_co2_kg_q25",
    ])

    embodied_high_col = get_existing_column(cols, [
        "q75_embodied_CO2_kg",
        "q75_embodied_co2_kg",
        "q75_room_embodied_CO2_kg",
        "q75_room_embodied_co2_kg",
        "embodied_CO2_kg_q75",
        "embodied_co2_kg_q75",
        "room_embodied_CO2_kg_q75",
        "room_embodied_co2_kg_q75",
    ])

    rows = conn.execute(
        f"""
        SELECT *
        FROM {quote_ident(TABLE_INVENTORY_ROOM_SNAPSHOT)}
        WHERE inventory_snapshot_id = ?;
        """,
        (inventory_snapshot_id,),
    ).fetchall()

    out: dict[str, RoomStockRecord] = {}

    for row in rows:
        data = dict(row)
        room_type = str(data["room_type"])

        embodied_default = data.get(embodied_default_col) if embodied_default_col else None
        embodied_low = data.get(embodied_low_col) if embodied_low_col else embodied_default
        embodied_high = data.get(embodied_high_col) if embodied_high_col else embodied_default

        out[room_type] = RoomStockRecord(
            inventory_snapshot_id=int(data["inventory_snapshot_id"]),
            room_type=room_type,
            room_description=data.get("room_description"),
            room_size_m2=data.get("room_size_m2"),
            carbon=CarbonStockEstimate(
                total_low_kgC=data.get("q25_total_carbon_kgC"),
                total_default_kgC=data.get("expected_total_carbon_kgC"),
                total_high_kgC=data.get("q75_total_carbon_kgC"),
                biogenic_low_kgC=data.get("q25_biog_carbon_kgC"),
                biogenic_default_kgC=data.get("expected_biog_carbon_kgC"),
                biogenic_high_kgC=data.get("q75_biog_carbon_kgC"),
                fossil_low_kgC=data.get("q25_fossil_carbon_kgC"),
                fossil_default_kgC=data.get("expected_fossil_carbon_kgC"),
                fossil_high_kgC=data.get("q75_fossil_carbon_kgC"),
            ),
            embodied_CO2=EmbodiedCO2Estimate(
                low_kg=embodied_low,
                default_kg=embodied_default,
                high_kg=embodied_high,
            ),
        )

    return out


def load_item_stock_lookup(
    conn: sqlite3.Connection,
    *,
    inventory_snapshot_id: int,
) -> dict[str, ItemStockRecord]:
    """
    Load item-level carbon stock lookup for single-item fires.
    """
    rows = conn.execute(
        f"""
        SELECT *
        FROM {quote_ident(TABLE_ITEM_CARBON_LOOKUP_VIEW)}
        WHERE inventory_snapshot_id = ?;
        """,
        (inventory_snapshot_id,),
    ).fetchall()

    out: dict[str, ItemStockRecord] = {}

    for row in rows:
        data = dict(row)
        item_name = str(data["item_name"])

        # Current view has central values only.  Use the same value for all
        # estimate cases until item-level ranges are added.
        total = data.get("item_total_carbon_kgC")
        biog = data.get("item_biog_carbon_kgC")
        fossil = data.get("item_fossil_carbon_kgC")

        out[item_name] = ItemStockRecord(
            inventory_snapshot_id=int(data["inventory_snapshot_id"]),
            item_name=item_name,
            carbon=CarbonStockEstimate(
                total_low_kgC=total,
                total_default_kgC=total,
                total_high_kgC=total,
                biogenic_low_kgC=biog,
                biogenic_default_kgC=biog,
                biogenic_high_kgC=biog,
                fossil_low_kgC=fossil,
                fossil_default_kgC=fossil,
                fossil_high_kgC=fossil,
            ),
        )

    return out


def load_dwelling_size_lookup(
    conn: sqlite3.Connection,
    *,
    inventory_snapshot_id: int,
) -> dict[str, Optional[float]]:
    """
    Load dwelling size lookup by dwelling_type.
    """
    rows = conn.execute(
        f"""
        SELECT dwelling_type, dwelling_size_m2
        FROM {quote_ident(TABLE_INVENTORY_DWELLING_SIZE_SNAPSHOT)}
        WHERE inventory_snapshot_id = ?;
        """,
        (inventory_snapshot_id,),
    ).fetchall()

    return {str(row["dwelling_type"]): row["dwelling_size_m2"] for row in rows}


# -----------------------------------------------------------------------------
# DERIVED LOOKUPS
# -----------------------------------------------------------------------------

def build_whole_dwelling_carbon(room_lookup: dict[str, RoomStockRecord]) -> CarbonStockEstimate:
    """
    Sum room-level carbon stock to create a whole-dwelling stock estimate.

    The fire-facing inventory snapshot contains one row per model room type.
    For the first deterministic model, the whole dwelling stock is approximated
    as the sum of these room-level stock estimates.
    """
    def sum_values(attr: str) -> Optional[float]:
        values = [getattr(room.carbon, attr) for room in room_lookup.values()]
        values = [v for v in values if v is not None]
        if not values:
            return None
        return float(sum(values))

    return CarbonStockEstimate(
        total_low_kgC=sum_values("total_low_kgC"),
        total_default_kgC=sum_values("total_default_kgC"),
        total_high_kgC=sum_values("total_high_kgC"),
        biogenic_low_kgC=sum_values("biogenic_low_kgC"),
        biogenic_default_kgC=sum_values("biogenic_default_kgC"),
        biogenic_high_kgC=sum_values("biogenic_high_kgC"),
        fossil_low_kgC=sum_values("fossil_low_kgC"),
        fossil_default_kgC=sum_values("fossil_default_kgC"),
        fossil_high_kgC=sum_values("fossil_high_kgC"),
    )


def build_whole_dwelling_embodied(room_lookup: dict[str, RoomStockRecord]) -> EmbodiedCO2Estimate:
    """
    Sum room-level embodied CO2 to create a whole-dwelling estimate.
    """
    def sum_values(attr: str) -> Optional[float]:
        values = [getattr(room.embodied_CO2, attr) for room in room_lookup.values()]
        values = [v for v in values if v is not None]
        if not values:
            return None
        return float(sum(values))

    return EmbodiedCO2Estimate(
        low_kg=sum_values("low_kg"),
        default_kg=sum_values("default_kg"),
        high_kg=sum_values("high_kg"),
    )


# -----------------------------------------------------------------------------
# OUTPUT WRITERS
# -----------------------------------------------------------------------------

def insert_stage1_results(
    conn: sqlite3.Connection,
    rows: list[Stage1ComponentResult],
) -> int:
    """
    Insert Stage 1 component result rows.
    """
    for row in rows:
        insert_dict_adaptive(conn, table=TABLE_STAGE1_RESULTS, values=row.to_insert_dict())
    return len(rows)


def insert_stage2_results(
    conn: sqlite3.Connection,
    rows: list[Stage2SpeciesResult],
) -> int:
    """
    Insert Stage 2 species-emissions rows.

    The insert is schema-adaptive through insert_dict_adaptive(), so extra fields
    on the dataclass are ignored if the current database table does not yet have
    matching columns.

    This makes it easier to develop the Stage 2 schema in small steps.
    """
    for row in rows:
        insert_dict_adaptive(conn, table=TABLE_STAGE2_RESULTS, values=row.to_insert_dict())

    return len(rows)


def insert_model_warnings(
    conn: sqlite3.Connection,
    warnings: list[FireModelWarning],
) -> int:
    """
    Insert fire-model warning rows.
    """
    for warning in warnings:
        insert_dict_adaptive(conn, table=TABLE_MODEL_WARNINGS, values=warning.to_insert_dict())
    return len(warnings)


def upsert_model_omission_summary(
    conn: sqlite3.Connection,
    *,
    model_name: str,
    model_version: str,
    input_type: str,
    omit_reason: str,
    omitted_count: int,
) -> None:
    """
    Store the latest emissions-model omission summary.

    This table records rows that reached the model-facing fire_events table but
    were skipped during emissions modelling.

    This is separate from fire_event_omission_summary, which records omissions
    during upstream fire-event resolution.
    """
    table = "fire_model_omission_summary"

    if table not in list_tables(conn):
        return

    conn.execute(
        f"""
        INSERT INTO {quote_ident(table)} (
            model_name,
            model_version,
            input_type,
            omit_reason,
            omitted_count,
            created_at_utc
        )
        VALUES (
            ?,
            ?,
            ?,
            ?,
            ?,
            strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        )
        ON CONFLICT(model_name, input_type, omit_reason)
        DO UPDATE SET
            model_version = excluded.model_version,
            omitted_count = excluded.omitted_count,
            created_at_utc = excluded.created_at_utc;
        """,
        (
            model_name,
            model_version,
            input_type,
            omit_reason,
            int(omitted_count),
        ),
    )


def insert_model_metadata(conn: sqlite3.Connection, metadata: dict[str, Any]) -> None:
    """
    Insert the single latest model metadata row.
    """
    values = dict(metadata)
    values.setdefault("metadata_id", 1)
    insert_dict_adaptive(conn, table=TABLE_MODEL_METADATA, values=values)
