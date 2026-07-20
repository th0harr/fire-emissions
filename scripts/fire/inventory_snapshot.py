"""
Build the current inventory snapshot inside the fire modelling database.

This script copies the inventory-derived lookup data required by the fire model
from an inventory database into a fire/test database.

Run from the project root, for example:

    # Dry-run / preview only
    python -m scripts.fire.inventory_snapshot ^
        --profile tom ^
        --source-db inventory_db ^
        --destination-db test_db

    # Apply snapshot rebuild
    python -m scripts.fire.inventory_snapshot ^
        --profile tom ^
        --source-db inventory_db ^
        --destination-db test_db ^
        --apply

Notes
-----
This is intentionally a destructive rebuild of the fire-side inventory snapshot.

The fire_db/test_db keeps only the current snapshot. If the source inventory
database changes, the snapshot should be rebuilt and downstream fire-event
validation / fire-impact modelling should be rerun.
"""

from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.ingest_utils import IngestLogEntry, record_ingest_run, utc_now_iso
from scripts.path_config import load_local_paths_config, resolve_db_path


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Source type recorded in fire_db.sources and fire_db.ingest_log.
SOURCE_TYPE = "inventory_snapshot"

# Human-readable source metadata.
SOURCE_DESCRIPTION = "Inventory database snapshot for fire model"
SOURCE_ORG = "internal"

# Source inventory_db tables required to build the snapshot.
REQUIRED_SOURCE_TABLES = {
    "furniture",
    "item_dictionary",
    "room",
    "room_count_summary",
    "room_carbon_stock",
    "room_embodied_CO2",
    "dwelling_size",
}

# Destination fire_db tables required to receive the snapshot.
REQUIRED_DESTINATION_TABLES = {
    "sources",
    "ingest_log",
    "inventory_snapshot",
    "inventory_furniture_snapshot",
    "inventory_item_snapshot",
    "inventory_room_snapshot",
    "inventory_dwelling_size_snapshot",
}

# Destination fire_db views expected to exist after init_fire_db.py.
REQUIRED_DESTINATION_VIEWS = {
    "v_inventory_item_carbon_lookup",
}


# Room types that don't exist as inventory archetypes.
# (should be ignored when checking whether the inventory DB room outputs)
INPUT_ONLY_ROOM_TYPES = {
    "unknown",
}


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------

@dataclass
class SnapshotPlan:
    """Stores preview information for an inventory snapshot rebuild."""

    source_db_path: Path
    destination_db_path: Path

    current_snapshot_rows: int = 0
    current_furniture_rows: int = 0
    current_item_rows: int = 0
    current_room_rows: int = 0
    current_dwelling_size_rows: int = 0

    source_furniture_rows: int = 0
    source_item_rows: int = 0
    source_room_rows: int = 0
    source_dwelling_size_rows: int = 0

    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def has_blocking_errors(self) -> bool:
        """Return True when the snapshot should not be applied."""
        return len(self.errors) > 0

    @property
    def total_existing_rows(self) -> int:
        """Total existing destination snapshot rows that would be deleted."""
        return (
            self.current_snapshot_rows
            + self.current_furniture_rows
            + self.current_item_rows
            + self.current_room_rows
            + self.current_dwelling_size_rows
        )

    @property
    def total_source_rows(self) -> int:
        """Total source rows that would be copied."""
        return (
            self.source_furniture_rows
            + self.source_item_rows
            + self.source_room_rows
            + self.source_dwelling_size_rows
        )


@dataclass
class SnapshotResult:
    """Stores the result of an applied inventory snapshot rebuild."""

    inventory_snapshot_id: int
    source_id: str

    furniture_rows: int
    item_rows: int
    room_rows: int
    dwelling_size_rows: int

    @property
    def total_rows_inserted(self) -> int:
        """Total snapshot rows inserted, excluding sources and ingest_log."""
        return (
            1  # inventory_snapshot row
            + self.furniture_rows
            + self.item_rows
            + self.room_rows
            + self.dwelling_size_rows
        )


# -----------------------------------------------------------------------------
# Public functions
# -----------------------------------------------------------------------------

def build_inventory_snapshot(
    source_db_path: str | Path,
    destination_db_path: str | Path,
    apply: bool = False,
) -> SnapshotPlan | SnapshotResult:
    """
    Build or preview the current inventory snapshot in the fire database.

    Parameters
    ----------
    source_db_path:
        Path to the source inventory_db SQLite file.

    destination_db_path:
        Path to the destination fire_db/test_db SQLite file.

    apply:
        If False, only previews the rebuild.
        If True, deletes the existing destination snapshot and rebuilds it.

    Returns
    -------
    SnapshotPlan | SnapshotResult
        Dry-run mode returns a SnapshotPlan.
        Apply mode returns a SnapshotResult.

    Notes
    -----
    This function is intentionally public so that it can later be called by:

        - scripts.fire.init_fire_db.py
        - scripts.model dispatcher
        - future setup/refresh workflows

    rather than duplicating the snapshot logic.
    """
    source_db_path = Path(source_db_path)
    destination_db_path = Path(destination_db_path)

    plan = plan_inventory_snapshot(
        source_db_path=source_db_path,
        destination_db_path=destination_db_path,
    )

    if not apply:
        return plan

    if plan.has_blocking_errors:
        raise RuntimeError(
            "Inventory snapshot was not applied because blocking errors were found:\n"
            + "\n".join(f"- {e}" for e in plan.errors)
        )

    return apply_inventory_snapshot(
        source_db_path=source_db_path,
        destination_db_path=destination_db_path,
    )


def plan_inventory_snapshot(
    source_db_path: str | Path,
    destination_db_path: str | Path,
) -> SnapshotPlan:
    """
    Perform a dry-run validation and preview of the inventory snapshot rebuild.

    This does not write to either database.
    """
    source_db_path = Path(source_db_path)
    destination_db_path = Path(destination_db_path)

    plan = SnapshotPlan(
        source_db_path=source_db_path,
        destination_db_path=destination_db_path,
    )

    # Basic file/path validation first, so later SQLite errors are clearer.
    if not source_db_path.exists():
        plan.errors.append(f"Source inventory database does not exist: {source_db_path}")

    if not destination_db_path.exists():
        plan.errors.append(f"Destination fire database does not exist: {destination_db_path}")

    # Avoid accidentally attaching/copying the same database file into itself.
    if source_db_path.exists() and destination_db_path.exists():
        if source_db_path.resolve() == destination_db_path.resolve():
            plan.errors.append(
                "Source and destination databases are the same file. "
                "Inventory snapshot requires two separate databases."
            )

    if plan.has_blocking_errors:
        return plan

    conn = _connect_destination_db(destination_db_path)

    try:
        # Attach the source inventory database as a read-only-ish attached DB.
        # SQLite does not make this truly read-only here, but all our queries
        # against inv.* are SELECT-only.
        _attach_inventory_db(conn, source_db_path)

        # Validate the fire_db schema before checking source tables.
        _validate_destination_schema(conn, plan)

        # Validate inventory_db schema and content.
        _validate_source_schema(conn, plan)
        _validate_source_required_values(conn, plan)

        # Count what currently exists in the destination.
        if not plan.has_blocking_errors:
            _populate_existing_snapshot_counts(conn, plan)
            _populate_source_counts(conn, plan)

    finally:
        conn.close()

    return plan


def apply_inventory_snapshot(
    source_db_path: str | Path,
    destination_db_path: str | Path,
) -> SnapshotResult:
    """
    Apply the inventory snapshot rebuild.

    This deletes the existing fire-side inventory snapshot and inserts a new one.
    The operation is transactional: if any step fails, the destination fire_db is
    rolled back to its previous state.
    """
    source_db_path = Path(source_db_path)
    destination_db_path = Path(destination_db_path)

    started = utc_now_iso()
    source_id = _make_source_id(started)

    conn = _connect_destination_db(destination_db_path)

    try:
        _attach_inventory_db(conn, source_db_path)

        # Re-run validation immediately before applying.
        # This avoids applying from a stale dry-run result.
        plan = SnapshotPlan(
            source_db_path=source_db_path,
            destination_db_path=destination_db_path,
        )
        _validate_destination_schema(conn, plan)
        _validate_source_schema(conn, plan)
        _validate_source_required_values(conn, plan)

        if plan.has_blocking_errors:
            raise RuntimeError(
                "Inventory snapshot was not applied because blocking errors were found:\n"
                + "\n".join(f"- {e}" for e in plan.errors)
            )

        try:
            conn.execute("BEGIN")

            # Delete existing snapshot metadata.
            # Child snapshot rows should be removed automatically via ON DELETE CASCADE.
            conn.execute("DELETE FROM inventory_snapshot")

            # Remove old inventory_snapshot source rows from sources.
            # This keeps the fire_db sources table tidy for the current-snapshot design.
            conn.execute(
                "DELETE FROM sources WHERE data_source_type = ?",
                (SOURCE_TYPE,),
            )

            # Create a new sources row.
            _insert_source_row(
                conn=conn,
                source_id=source_id,
                source_db_path=source_db_path,
                started_utc=started,
            )

            # Create a new inventory_snapshot row and fetch its generated ID.
            inventory_snapshot_id = _insert_inventory_snapshot_row(
                conn=conn,
                source_id=source_id,
                source_db_path=source_db_path,
                started_utc=started,
            )

            # Copy inventory-derived lookup values into fire_db.
            furniture_rows = _copy_furniture_snapshot(conn, inventory_snapshot_id)
            item_rows = _copy_item_snapshot(conn, inventory_snapshot_id)
            room_rows = _copy_room_snapshot(conn, inventory_snapshot_id)
            dwelling_size_rows = _copy_dwelling_size_snapshot(conn, inventory_snapshot_id)

            conn.commit()

        except Exception:
            conn.rollback()
            raise

        result = SnapshotResult(
            inventory_snapshot_id=inventory_snapshot_id,
            source_id=source_id,
            furniture_rows=furniture_rows,
            item_rows=item_rows,
            room_rows=room_rows,
            dwelling_size_rows=dwelling_size_rows,
        )

        # Log success after the main transaction has committed.
        _record_snapshot_log(
            conn=conn,
            source_id=source_id,
            status="success",
            started_utc=started,
            message=(
                f"Built inventory snapshot {inventory_snapshot_id} from {source_db_path}. "
                f"Copied {furniture_rows} furniture classes, {item_rows} items, "
                f"{room_rows} rooms, and {dwelling_size_rows} dwelling size rows."
            ),
            rows_inserted=result.total_rows_inserted,
        )
        conn.commit()

        return result

    except Exception as exc:
        # Try to record failure. This should not mask the original error.
        try:
            _record_snapshot_log(
                conn=conn,
                source_id=source_id,
                status="failed",
                started_utc=started,
                message=str(exc),
                rows_inserted=None,
            )
            conn.commit()
        except Exception:
            pass

        raise

    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Connection helpers
# -----------------------------------------------------------------------------

def _connect_destination_db(destination_db_path: Path) -> sqlite3.Connection:
    """
    Open the destination fire database.

    The destination is where we write, so this connection controls the transaction.
    """
    conn = sqlite3.connect(destination_db_path)
    conn.row_factory = sqlite3.Row

    # Foreign keys are required so deleting inventory_snapshot cascades to the
    # child snapshot tables.
    conn.execute("PRAGMA foreign_keys = ON;")

    return conn


def _attach_inventory_db(conn: sqlite3.Connection, source_db_path: Path) -> None:
    """
    Attach the inventory database as schema name 'inv'.

    After this, inventory tables are queried as:
        inv.item_dictionary
        inv.furniture
        inv.room
        etc.
    """
    # SQLite ATTACH cannot parameterise the schema name, but can parameterise path.
    conn.execute("ATTACH DATABASE ? AS inv", (str(source_db_path),))


# -----------------------------------------------------------------------------
# Validation helpers
# -----------------------------------------------------------------------------

def _validate_destination_schema(conn: sqlite3.Connection, plan: SnapshotPlan) -> None:
    """
    Check that destination fire_db has been initialised with the expected schema.
    """
    destination_tables = _list_tables(conn, schema_name="main")
    destination_views = _list_views(conn, schema_name="main")

    missing_tables = sorted(REQUIRED_DESTINATION_TABLES - destination_tables)
    missing_views = sorted(REQUIRED_DESTINATION_VIEWS - destination_views)

    for table in missing_tables:
        plan.errors.append(
            f"Destination fire database is missing required table: {table}. "
            "Run scripts.fire.init_fire_db first."
        )

    for view in missing_views:
        plan.errors.append(
            f"Destination fire database is missing required view: {view}. "
            "Run scripts.fire.init_fire_db first."
        )


def make_input_only_room_filter_sql(
    *,
    table_alias: str = "r",
) -> tuple[str, tuple]:
    """
    Build a SQL filter that excludes input-only room types from model-readiness
    checks.

    This keeps special-case room types such as 'unknown' available in the room
    vocabulary, while preventing the snapshot builder from incorrectly treating
    them as missing inventory-model outputs.
    """
    excluded_room_types = sorted(INPUT_ONLY_ROOM_TYPES)

    if not excluded_room_types:
        return "1 = 1", tuple()

    placeholders = ", ".join("?" for _ in excluded_room_types)

    return (
        f"{table_alias}.room_type NOT IN ({placeholders})",
        tuple(excluded_room_types),
    )


def _validate_source_schema(conn: sqlite3.Connection, plan: SnapshotPlan) -> None:
    """
    Check that the attached inventory_db contains the required source tables.
    """
    source_tables = _list_tables(conn, schema_name="inv")
    missing_tables = sorted(REQUIRED_SOURCE_TABLES - source_tables)

    for table in missing_tables:
        plan.errors.append(
            f"Source inventory database is missing required table: {table}."
        )

    if missing_tables:
        return

    # Required tables must not be empty.
    for table in sorted(REQUIRED_SOURCE_TABLES):
        n = _count_rows(conn, f"inv.{table}")
        if n == 0:
            plan.errors.append(
                f"Source inventory table is empty: {table}."
            )


def _validate_source_required_values(conn: sqlite3.Connection, plan: SnapshotPlan) -> None:
    """
    Check for missing values that would make the snapshot unusable downstream.

    These are blocking errors, not warnings, because missing item masses,
    furniture carbon factors, room sizes, or room carbon stocks will break
    fire impact calculations.
    """
    if plan.has_blocking_errors:
        return

    # The source room vocabulary includes a small number of input-only room
    # types, such as `unknown`. 
    # These are valid fire-input values, but they are not inventory archetypes.
    # Therefore, when validating room-level model outputs, we deliberately
    # exclude these input-only rows.
    # Otherwise the snapshot dry-run would incorrectly fail because `unknown`
    # has no room_count_summary or room_carbon_stock row.
    room_filter_sql, room_filter_params = make_input_only_room_filter_sql(
        table_alias="r",
    )

    checks = [
        (
            "item_dictionary.item_mass",
            """
            SELECT COUNT(*) AS n
            FROM inv.item_dictionary
            WHERE item_mass IS NULL
            """,
        ),
        (
            "item_dictionary.furniture_class",
            """
            SELECT COUNT(*) AS n
            FROM inv.item_dictionary
            WHERE furniture_class IS NULL
               OR TRIM(furniture_class) = ''
            """,
        ),
        (
            "item_dictionary.furniture_class values missing from furniture",
            """
            SELECT COUNT(*) AS n
            FROM inv.item_dictionary AS i
            LEFT JOIN inv.furniture AS f
                ON i.furniture_class = f.furniture_class
            WHERE i.furniture_class IS NOT NULL
              AND TRIM(i.furniture_class) <> ''
              AND f.furniture_class IS NULL
            """,
        ),
        (
            "furniture.kgC_kg",
            """
            SELECT COUNT(*) AS n
            FROM inv.furniture
            WHERE kgC_kg IS NULL
            """,
        ),
        (
            "furniture.ratio_fossil",
            """
            SELECT COUNT(*) AS n
            FROM inv.furniture
            WHERE ratio_fossil IS NULL
            """,
        ),
        (
            "furniture.ratio_biog",
            """
            SELECT COUNT(*) AS n
            FROM inv.furniture
            WHERE ratio_biog IS NULL
            """,
        ),
        (
            "room.room_size_m2",
            f"""
            SELECT COUNT(*) AS n
            FROM inv.room AS r
            WHERE {room_filter_sql}
              AND r.room_size_m2 IS NULL
            """,
            room_filter_params,
        ),
        (
            "room rows missing room_count_summary",
            f"""
            SELECT COUNT(*) AS n
            FROM inv.room AS r
            LEFT JOIN inv.room_count_summary AS rcs
                ON r.room_type = rcs.room_type
            WHERE {room_filter_sql}
              AND rcs.room_type IS NULL
            """,
            room_filter_params,
        ),
        (
            "room rows missing room_carbon_stock",
            f"""
            SELECT COUNT(*) AS n
            FROM inv.room AS r
            LEFT JOIN inv.room_carbon_stock AS rcs
                ON r.room_type = rcs.room_type
            WHERE {room_filter_sql}
              AND rcs.room_type IS NULL
            """,
            room_filter_params,
        ),
        (
            "room_count_summary.expected_count_mean",
            """
            SELECT COUNT(*) AS n
            FROM inv.room_count_summary
            WHERE expected_count_mean IS NULL
            """,
        ),
        (
            "room_carbon_stock.expected_total_carbon_kgC",
            """
            SELECT COUNT(*) AS n
            FROM inv.room_carbon_stock
            WHERE expected_total_carbon_kgC IS NULL
            """,
        ),
        (
            "room_carbon_stock.expected_biog_carbon_kgC",
            """
            SELECT COUNT(*) AS n
            FROM inv.room_carbon_stock
            WHERE expected_biog_carbon_kgC IS NULL
            """,
        ),
        (
            "room_carbon_stock.expected_fossil_carbon_kgC",
            """
            SELECT COUNT(*) AS n
            FROM inv.room_carbon_stock
            WHERE expected_fossil_carbon_kgC IS NULL
            """,
        ),
        (
            "room rows missing room_embodied_CO2",
            f"""
            SELECT COUNT(*) AS n
            FROM inv.room AS r
            LEFT JOIN inv.room_embodied_CO2 AS reco2
                ON r.room_type = reco2.room_type
            WHERE {room_filter_sql}
              AND reco2.room_type IS NULL
            """,
            room_filter_params,
        ),
        (
            "room_embodied_CO2.expected_embodied_CO2_kg",
            """
            SELECT COUNT(*) AS n
            FROM inv.room_embodied_CO2
            WHERE expected_embodied_CO2_kg IS NULL
            """,
        ),
        (
            "room_embodied_CO2.q25_embodied_CO2_kg",
            """
            SELECT COUNT(*) AS n
            FROM inv.room_embodied_CO2
            WHERE q25_embodied_CO2_kg IS NULL
            """,
        ),
        (
            "room_embodied_CO2.q75_embodied_CO2_kg",
            """
            SELECT COUNT(*) AS n
            FROM inv.room_embodied_CO2
            WHERE q75_embodied_CO2_kg IS NULL
            """,
        ),
        (
            "dwelling_size.dwelling_size_m2",
            """
            SELECT COUNT(*) AS n
            FROM inv.dwelling_size
            WHERE dwelling_size_m2 IS NULL
            """,
        ),
    ]

    for check in checks:
        # Most validation checks are simple no-parameter SQL queries.
        # Room-level checks use parameters so that input-only room types
        # can be excluded safely, rather than interpolated into SQL strings.
        if len(check) == 2:
            label, sql = check
            params = tuple()
        else:
            label, sql, params = check

        n = int(conn.execute(sql, params).fetchone()["n"])
        if n > 0:
            plan.errors.append(
                f"Missing required value in source inventory database: {label} "
                f"({n} affected row(s))."
            )


# -----------------------------------------------------------------------------
# Preview helpers
# -----------------------------------------------------------------------------

def _populate_existing_snapshot_counts(
    conn: sqlite3.Connection,
    plan: SnapshotPlan,
) -> None:
    """
    Count existing destination snapshot rows that would be removed by --apply.
    """
    plan.current_snapshot_rows = _count_rows(conn, "inventory_snapshot")
    plan.current_furniture_rows = _count_rows(conn, "inventory_furniture_snapshot")
    plan.current_item_rows = _count_rows(conn, "inventory_item_snapshot")
    plan.current_room_rows = _count_rows(conn, "inventory_room_snapshot")
    plan.current_dwelling_size_rows = _count_rows(conn, "inventory_dwelling_size_snapshot")


def _populate_source_counts(
    conn: sqlite3.Connection,
    plan: SnapshotPlan,
) -> None:
    """
    Count source rows that would be copied into the destination snapshot.
    """
    plan.source_furniture_rows = _count_rows(conn, "inv.furniture")
    plan.source_item_rows = _count_rows(conn, "inv.item_dictionary")

    # Count the room rows that would actually be copied into the fire-side
    # snapshot. Input-only room values such as ``unknown`` remain valid fire
    # input vocabulary, but they are intentionally not copied into the
    # model-ready room snapshot because they have no inventory carbon stock.
    room_filter_sql, room_filter_params = make_input_only_room_filter_sql(
        table_alias="r",
    )    

    plan.source_room_rows = int(conn.execute(
        f"""
        SELECT COUNT(*) AS n
        FROM inv.room AS r
        JOIN inv.room_count_summary AS rcount
            ON r.room_type = rcount.room_type
        JOIN inv.room_carbon_stock AS rcarbon
            ON r.room_type = rcarbon.room_type
        JOIN inv.room_embodied_CO2 AS reco2
            ON r.room_type = reco2.room_type
        WHERE {room_filter_sql}
        """,
        room_filter_params,
    ).fetchone()["n"])

    plan.source_dwelling_size_rows = _count_rows(conn, "inv.dwelling_size")


# -----------------------------------------------------------------------------
# Insert / copy helpers
# -----------------------------------------------------------------------------

def _insert_source_row(
    conn: sqlite3.Connection,
    source_id: str,
    source_db_path: Path,
    started_utc: str,
) -> None:
    """
    Insert one source row for this inventory snapshot build.
    """
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
            source_db_path.name,
            str(source_db_path),
            None,
            None,
            started_utc,
            "Current inventory lookup snapshot for fire modelling.",
        ),
    )


def _insert_inventory_snapshot_row(
    conn: sqlite3.Connection,
    source_id: str,
    source_db_path: Path,
    started_utc: str,
) -> int:
    """
    Insert the inventory_snapshot metadata row and return its generated ID.
    """
    cur = conn.execute(
        """
        INSERT INTO inventory_snapshot (
            source_id,
            source_inventory_db,
            date_imported_utc
        ) VALUES (?, ?, ?)
        """,
        (
            source_id,
            str(source_db_path),
            started_utc,
        ),
    )

    return int(cur.lastrowid)


def _copy_furniture_snapshot(
    conn: sqlite3.Connection,
    inventory_snapshot_id: int,
) -> int:
    """
    Copy furniture carbon factors from inventory_db into fire_db.
    """
    cur = conn.execute(
        """
        INSERT INTO inventory_furniture_snapshot (
            inventory_snapshot_id,
            furniture_class,
            kgC_kg,
            ratio_fossil,
            ratio_biog
        )
        SELECT
            ? AS inventory_snapshot_id,
            furniture_class,
            kgC_kg,
            ratio_fossil,
            ratio_biog
        FROM inv.furniture
        """,
        (inventory_snapshot_id,),
    )

    return _rowcount(cur)


def _copy_item_snapshot(
    conn: sqlite3.Connection,
    inventory_snapshot_id: int,
) -> int:
    """
    Copy item mass and furniture class from inventory_db into fire_db.
    """
    cur = conn.execute(
        """
        INSERT INTO inventory_item_snapshot (
            inventory_snapshot_id,
            item_name,
            item_mass_kg,
            furniture_class
        )
        SELECT
            ? AS inventory_snapshot_id,
            item_name,
            item_mass AS item_mass_kg,
            furniture_class
        FROM inv.item_dictionary
        """,
        (inventory_snapshot_id,),
    )

    return _rowcount(cur)


def _copy_room_snapshot(
    conn: sqlite3.Connection,
    inventory_snapshot_id: int,
) -> int:
    """
    Copy room vocabulary, room count summaries and room carbon stock.

    This creates one fire-facing room lookup table from the inventory-side room,
    room_count_summary and room_carbon_stock tables.
    """
    room_filter_sql, room_filter_params = make_input_only_room_filter_sql(
        table_alias="r",
    )

    cur = conn.execute(
        f"""
        INSERT INTO inventory_room_snapshot (
            inventory_snapshot_id,

            room_type,
            room_description,
            room_size_m2,

            expected_count_mean,
            count_q25,
            count_q75,

            expected_total_carbon_kgC,
            expected_biog_carbon_kgC,
            expected_fossil_carbon_kgC,
            expected_embodied_CO2_kg,

            q25_total_carbon_kgC,
            q25_biog_carbon_kgC,
            q25_fossil_carbon_kgC,
            q25_embodied_CO2_kg,

            q75_total_carbon_kgC,
            q75_biog_carbon_kgC,
            q75_fossil_carbon_kgC,
            q75_embodied_CO2_kg
        )
        SELECT
            ? AS inventory_snapshot_id,

            r.room_type,
            r.room_description,
            r.room_size_m2,

            rcount.expected_count_mean,
            rcount.count_q25,
            rcount.count_q75,

            rcarbon.expected_total_carbon_kgC,
            rcarbon.expected_biog_carbon_kgC,
            rcarbon.expected_fossil_carbon_kgC,
            reco2.expected_embodied_CO2_kg,

            rcarbon.q25_total_carbon_kgC,
            rcarbon.q25_biog_carbon_kgC,
            rcarbon.q25_fossil_carbon_kgC,
            reco2.q25_embodied_CO2_kg,

            rcarbon.q75_total_carbon_kgC,
            rcarbon.q75_biog_carbon_kgC,
            rcarbon.q75_fossil_carbon_kgC,
            reco2.q75_embodied_CO2_kg

        FROM inv.room AS r

        JOIN inv.room_count_summary AS rcount
            ON r.room_type = rcount.room_type

        JOIN inv.room_carbon_stock AS rcarbon
            ON r.room_type = rcarbon.room_type
        
        JOIN inv.room_embodied_CO2 AS reco2
            ON r.room_type = reco2.room_type

        WHERE {room_filter_sql}
        """,
        (inventory_snapshot_id, *room_filter_params),
    )

    return _rowcount(cur)


def _copy_dwelling_size_snapshot(
    conn: sqlite3.Connection,
    inventory_snapshot_id: int,
) -> int:
    """
    Copy dwelling size and dwelling type PMF from inventory_db into fire_db.
    """
    cur = conn.execute(
        """
        INSERT INTO inventory_dwelling_size_snapshot (
            inventory_snapshot_id,
            dwelling_type,
            dwelling_size_m2,
            count_value,
            dwelling_type_pmf
        )
        SELECT
            ? AS inventory_snapshot_id,
            dwelling_type,
            dwelling_size_m2,
            count_value,
            dwelling_type_pmf
        FROM inv.dwelling_size
        """,
        (inventory_snapshot_id,),
    )

    return _rowcount(cur)


# -----------------------------------------------------------------------------
# Logging helpers
# -----------------------------------------------------------------------------

def _record_snapshot_log(
    conn: sqlite3.Connection,
    source_id: str,
    status: str,
    started_utc: str,
    message: str,
    rows_inserted: int | None,
) -> None:
    """
    Record the inventory snapshot run in ingest_log.

    Uses the shared schema-tolerant logging helper.
    """
    record_ingest_run(
        conn,
        IngestLogEntry(
            source_id=source_id,
            data_source_type=SOURCE_TYPE,
            action="build_snapshot",
            status=status,
            message=message,
            started_utc=started_utc,
            finished_utc=utc_now_iso(),
            rows_inserted=rows_inserted,
        ),
    )


# -----------------------------------------------------------------------------
# Generic DB helpers
# -----------------------------------------------------------------------------

def _list_tables(conn: sqlite3.Connection, schema_name: str) -> set[str]:
    """
    Return the table names in a SQLite schema.

    schema_name is usually:
        - "main" for the destination fire_db
        - "inv" for the attached inventory_db
    """
    rows = conn.execute(
        f"""
        SELECT name
        FROM {schema_name}.sqlite_master
        WHERE type = 'table'
        """
    ).fetchall()

    return {str(r["name"]) for r in rows}


def _list_views(conn: sqlite3.Connection, schema_name: str) -> set[str]:
    """
    Return the view names in a SQLite schema.
    """
    rows = conn.execute(
        f"""
        SELECT name
        FROM {schema_name}.sqlite_master
        WHERE type = 'view'
        """
    ).fetchall()

    return {str(r["name"]) for r in rows}


def _count_rows(conn: sqlite3.Connection, table_name: str) -> int:
    """
    Count rows in a table.

    table_name may include a schema prefix, e.g.:
        inv.item_dictionary
    """
    row = conn.execute(f"SELECT COUNT(*) AS n FROM {table_name}").fetchone()
    return int(row["n"])


def _rowcount(cur: sqlite3.Cursor) -> int:
    """
    Return SQLite cursor rowcount as a non-negative integer.
    """
    if cur.rowcount is None or cur.rowcount == -1:
        return 0
    return int(cur.rowcount)


def _make_source_id(started_utc: str) -> str:
    """
    Create a simple unique source_id for this snapshot run.

    Uses a filesystem/SQLite-friendly timestamp string.
    """
    safe_time = (
        started_utc
        .replace("-", "")
        .replace(":", "")
        .replace("+00:00", "Z")
        .replace(" ", "_")
    )

    return f"{SOURCE_TYPE}_{safe_time}"


# -----------------------------------------------------------------------------
# Reporting helpers
# -----------------------------------------------------------------------------

def print_snapshot_plan(plan: SnapshotPlan) -> None:
    """
    Print a human-readable dry-run summary.
    """
    print("\nInventory snapshot preview")
    print("--------------------------")
    print(f"Source DB:      {plan.source_db_path}")
    print(f"Destination DB: {plan.destination_db_path}")

    print("\nExisting destination snapshot rows that would be deleted:")
    print(f"  inventory_snapshot:                 {plan.current_snapshot_rows}")
    print(f"  inventory_furniture_snapshot:       {plan.current_furniture_rows}")
    print(f"  inventory_item_snapshot:            {plan.current_item_rows}")
    print(f"  inventory_room_snapshot:            {plan.current_room_rows}")
    print(f"  inventory_dwelling_size_snapshot:   {plan.current_dwelling_size_rows}")
    print(f"  Total existing rows:                {plan.total_existing_rows}")

    print("\nSource inventory rows that would be copied:")
    print(f"  furniture:                          {plan.source_furniture_rows}")
    print(f"  item_dictionary:                    {plan.source_item_rows}")
    print(f"  room:                               {plan.source_room_rows}")
    print(f"  dwelling_size:                      {plan.source_dwelling_size_rows}")
    print(f"  Total source rows:                  {plan.total_source_rows}")

    if plan.errors:
        print("\nBlocking errors:")
        for err in plan.errors:
            print(f"  - {err}")

    if plan.warnings:
        print("\nWarnings:")
        for warn in plan.warnings:
            print(f"  - {warn}")

    if not plan.errors:
        print("\nDry run complete. Re-run with --apply to rebuild the snapshot.")


def print_snapshot_result(result: SnapshotResult) -> None:
    """
    Print a human-readable apply summary.
    """
    print("\nInventory snapshot applied")
    print("--------------------------")
    print(f"inventory_snapshot_id: {result.inventory_snapshot_id}")
    print(f"source_id:             {result.source_id}")

    print("\nRows copied:")
    print(f"  furniture classes:   {result.furniture_rows}")
    print(f"  items:               {result.item_rows}")
    print(f"  rooms:               {result.room_rows}")
    print(f"  dwelling sizes:      {result.dwelling_size_rows}")
    print(f"  total rows inserted: {result.total_rows_inserted}")


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="inventory_snapshot",
        description=(
            "Build the current inventory lookup snapshot in a fire/test database. "
            "Dry-run preview is the default; use --apply to write changes."
        ),
    )

    parser.add_argument(
        "--profile",
        required=True,
        help="Profile name from config/local_paths.yaml, e.g. tom.",
    )

    parser.add_argument(
        "--source-db",
        required=True,
        help="Source inventory database handle, e.g. inventory_db.",
    )

    parser.add_argument(
        "--destination-db",
        required=True,
        help="Destination fire/test database handle, e.g. test_db or fire_db.",
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the snapshot rebuild. Without this flag, only a dry-run preview is shown.",
    )

    args = parser.parse_args(argv)

    config = load_local_paths_config(Path("config") / "local_paths.yaml")

    source = resolve_db_path(
        profile=args.profile,
        db_handle=args.source_db,
        config=config,
    )

    destination = resolve_db_path(
        profile=args.profile,
        db_handle=args.destination_db,
        config=config,
    )

    result = build_inventory_snapshot(
        source_db_path=source.db_path,
        destination_db_path=destination.db_path,
        apply=args.apply,
    )

    if isinstance(result, SnapshotPlan):
        print_snapshot_plan(result)
        return 1 if result.has_blocking_errors else 0

    print_snapshot_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())