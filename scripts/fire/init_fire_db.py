"""
Initialise a blank SQLite database for the Fire Emissions fire modelling project.

Note:
    This creates the database structure only.
    It does not ingest fire input files or copy inventory snapshots.

Run from the project root, for example:
    python -m scripts.fire.init_fire_db --profile tom --db fire_db
"""

import argparse
import sqlite3
from pathlib import Path

from scripts.path_config import load_local_paths_config


# FUNCTION: Create the intended DB filepath
def resolve_db_path(profile: str, db_handle: str, config: dict) -> Path:
    """
    Resolve the full SQLite database path for the selected profile and DB handle
    from config/local_paths.yaml.

    This mirrors the path-resolution logic used by init_inventory_db.py.
    """

    # Build DB root directory
    profiles = config.get("profiles", {})
    db_roots = config.get("db_roots", {})

    # Validation
    if profile not in profiles:
        raise KeyError(
            f"Profile '{profile}' not found in config.\n"
            f"Available profiles: {', '.join(sorted(profiles.keys())) or '(none)'}"
        )

    if db_handle not in db_roots:
        raise KeyError(
            f"DB handle '{db_handle}' not found in config.\n"
            f"Available db handles: {', '.join(sorted(db_roots.keys())) or '(none)'}"
        )

    # Build specific DB filepath
    sharepoint_root = Path(profiles[profile]["sharepoint_root"])
    db_cfg = db_roots[db_handle]

    root = db_cfg.get("root")
    rel_db = db_cfg.get("rel_db")

    # Validation
    if not root:
        raise KeyError(f"Missing required db_roots.{db_handle}.root in config.")

    if not rel_db:
        raise KeyError(f"Missing required db_roots.{db_handle}.rel_db in config.")

    return sharepoint_root / Path(root) / Path(rel_db)


# FUNCTION: Create a blank fire modelling database
def init_database(sqlite_path: str) -> None:
    """
    Create the initial fire_db database structure.

    This first-pass fire database contains:

    1. sources
       - General source/import tracking table.
       - Kept similar to inventory_db so shared ingest utilities can be reused.

    2. ingest_log
       - Audit trail for ingest/model operations.
       - Kept compatible with scripts.ingest_utils.record_ingest_run().

    3. fire_event_parameter_input
       - Raw/staging table for the fire_input_param workbook's inputs sheet.
       - This records what the user supplied.
       - It does NOT resolve canonical room_type, item_name, or model-ready values.

    4. inventory snapshot tables
       - Fire-side snapshots of the inventory-derived values required by the
         fire model.
       - These are populated later by a separate snapshot/refresh module.

    5. model-facing inventory views
       - Convenience views for common calculated lookups.
    """

    db_path = Path(sqlite_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Opens connection to database file OR creates it
    con = sqlite3.connect(db_path)

    try:
        cur = con.cursor()

        # Enable defaults
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute("PRAGMA journal_mode = WAL;")

        # -----------------------------------------
        # SOURCES
        # General source/import tracking
        # -----------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS sources (
            source_id TEXT PRIMARY KEY,
            data_source_type TEXT NOT NULL,
            source_description TEXT,
            source_org TEXT,
            file_name TEXT,
            file_path TEXT,
            url TEXT,
            date_collected TEXT,
            date_imported_utc TEXT NOT NULL,
            notes TEXT
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_sources_data_source_type
            ON sources (data_source_type);
        """)

        # -----------------------
        # INGEST LOG
        # Simple audit trail
        # -----------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ingest_log (
            ingest_id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id TEXT,
            data_source_type TEXT,
            action TEXT,
            status TEXT,
            message TEXT,
            started_utc TEXT,
            finished_utc TEXT,
            rows_inserted INTEGER,
            rows_deleted INTEGER,
            FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE SET NULL
        );
        """)

        # -------------------------------------------------
        # FIRE EVENT PARAMETER INPUT
        # Raw/staging table for one fire input workbook
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_event_parameter_input (
            staging_id INTEGER PRIMARY KEY AUTOINCREMENT,

            source_id TEXT NOT NULL,

            input_row INTEGER,
            fire_parameter TEXT NOT NULL,

            value_text TEXT,
            value_numeric REAL,
            value_bool INTEGER,
            unit TEXT,

            input_notes TEXT,

            FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE,

            CHECK (
                value_bool IS NULL
                OR value_bool IN (0, 1)
            )
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_parameter_input_source
            ON fire_event_parameter_input (source_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_parameter_input_parameter
            ON fire_event_parameter_input (fire_parameter);
        """)



        # -------------------------------------------------
        # FIRE INPUT VALUE MAPPING
        # Mapping from user-facing input values to canonical model values.
        #
        # Example:
        #   Input = "Single item only"
        #   Canonical naming = "single_item"
        #   name_category = "fire_spread_category"
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_input_value_mapping (
            mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,

            mapping_row INTEGER,

            input_value TEXT NOT NULL,
            canonical_value TEXT NOT NULL,
            name_category TEXT NOT NULL,

            UNIQUE (name_category, input_value)
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_input_value_mapping_category
            ON fire_input_value_mapping (name_category, input_value);
        """)


        # -------------------------------------------------
        # FIRE IGNITION ITEM MAPPING
        # Mapping from FRIS ignition-source labels to inventory item names.
        #
        # Used later when:
        #   fire_spread_category = single_item
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_ignition_item_mapping (
            mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,

            mapping_row INTEGER,

            ignition_source TEXT NOT NULL,
            ignition_source_category TEXT,

            single_item_status TEXT NOT NULL,
            item_combusted TEXT,

            mapping_notes TEXT,

            CHECK (
                single_item_status IN (
                    'direct_inventory_item',
                    'proxy_inventory_item',
                    'invalid_single_item',
                    'unmapped'
                )
            ),

            CHECK (
                (
                    single_item_status IN ('direct_inventory_item', 'proxy_inventory_item')
                    AND item_combusted IS NOT NULL
                    AND TRIM(item_combusted) <> ''
                )
                OR
                (
                    single_item_status IN ('invalid_single_item', 'unmapped')
                    AND (
                        item_combusted IS NULL
                        OR TRIM(item_combusted) = ''
                    )
                )
            ),

            UNIQUE (ignition_source_category, ignition_source)
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_ignition_item_mapping_source
            ON fire_ignition_item_mapping (ignition_source_category, ignition_source);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_ignition_item_mapping_item
            ON fire_ignition_item_mapping (item_combusted);
        """)


        # -------------------------------------------------
        # INVENTORY SNAPSHOT
        # Metadata for copied/derived inventory lookup values
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory_snapshot (
            inventory_snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,

            source_id TEXT NOT NULL,
            source_inventory_db TEXT,
            date_imported_utc TEXT NOT NULL,

            FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_inventory_snapshot_source
            ON inventory_snapshot (source_id);
        """)

        # -------------------------------------------------
        # INVENTORY FURNITURE SNAPSHOT
        # Snapshot of furniture carbon factors
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory_furniture_snapshot (
            inventory_snapshot_id INTEGER NOT NULL,

            furniture_class TEXT NOT NULL,
            kgC_kg REAL,
            ratio_fossil REAL,
            ratio_biog REAL,

            PRIMARY KEY (inventory_snapshot_id, furniture_class),

            FOREIGN KEY (inventory_snapshot_id)
                REFERENCES inventory_snapshot(inventory_snapshot_id)
                ON DELETE CASCADE,

            CHECK (kgC_kg IS NULL OR kgC_kg >= 0.0),
            CHECK (ratio_fossil IS NULL OR ratio_fossil >= 0.0),
            CHECK (ratio_biog IS NULL OR ratio_biog >= 0.0)
        );
        """)

        # -------------------------------------------------
        # INVENTORY ITEM SNAPSHOT
        # Snapshot of item mass and furniture class
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory_item_snapshot (
            inventory_snapshot_id INTEGER NOT NULL,

            item_name TEXT NOT NULL,
            item_mass_kg REAL,
            furniture_class TEXT,

            PRIMARY KEY (inventory_snapshot_id, item_name),

            FOREIGN KEY (inventory_snapshot_id)
                REFERENCES inventory_snapshot(inventory_snapshot_id)
                ON DELETE CASCADE,

            FOREIGN KEY (inventory_snapshot_id, furniture_class)
                REFERENCES inventory_furniture_snapshot (
                    inventory_snapshot_id,
                    furniture_class
                ),

            CHECK (item_mass_kg IS NULL OR item_mass_kg >= 0.0)
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_inventory_item_snapshot_furniture_class
            ON inventory_item_snapshot (inventory_snapshot_id, furniture_class);
        """)

        # -------------------------------------------------
        # INVENTORY ROOM SNAPSHOT
        # Combined fire-facing room lookup:
        # room vocab + room size + expected count + carbon stock
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory_room_snapshot (
            inventory_snapshot_id INTEGER NOT NULL,

            room_type TEXT NOT NULL,
            room_description TEXT NOT NULL,
            room_size_m2 REAL,

            expected_count_mean REAL,
            count_q25 REAL,
            count_q75 REAL,

            expected_total_carbon_kgC REAL,
            expected_biog_carbon_kgC REAL,
            expected_fossil_carbon_kgC REAL,

            q25_total_carbon_kgC REAL,
            q25_biog_carbon_kgC REAL,
            q25_fossil_carbon_kgC REAL,

            q75_total_carbon_kgC REAL,
            q75_biog_carbon_kgC REAL,
            q75_fossil_carbon_kgC REAL,

            PRIMARY KEY (inventory_snapshot_id, room_type),

            UNIQUE (inventory_snapshot_id, room_description),

            FOREIGN KEY (inventory_snapshot_id)
                REFERENCES inventory_snapshot(inventory_snapshot_id)
                ON DELETE CASCADE,

            CHECK (room_size_m2 IS NULL OR room_size_m2 >= 0.0),

            CHECK (expected_count_mean IS NULL OR expected_count_mean >= 0.0),
            CHECK (count_q25 IS NULL OR count_q25 >= 0.0),
            CHECK (count_q75 IS NULL OR count_q75 >= 0.0),

            CHECK (expected_total_carbon_kgC IS NULL OR expected_total_carbon_kgC >= 0.0),
            CHECK (expected_biog_carbon_kgC IS NULL OR expected_biog_carbon_kgC >= 0.0),
            CHECK (expected_fossil_carbon_kgC IS NULL OR expected_fossil_carbon_kgC >= 0.0),

            CHECK (q25_total_carbon_kgC IS NULL OR q25_total_carbon_kgC >= 0.0),
            CHECK (q25_biog_carbon_kgC IS NULL OR q25_biog_carbon_kgC >= 0.0),
            CHECK (q25_fossil_carbon_kgC IS NULL OR q25_fossil_carbon_kgC >= 0.0),

            CHECK (q75_total_carbon_kgC IS NULL OR q75_total_carbon_kgC >= 0.0),
            CHECK (q75_biog_carbon_kgC IS NULL OR q75_biog_carbon_kgC >= 0.0),
            CHECK (q75_fossil_carbon_kgC IS NULL OR q75_fossil_carbon_kgC >= 0.0)
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_inventory_room_snapshot_description
            ON inventory_room_snapshot (inventory_snapshot_id, room_description);
        """)

        # -------------------------------------------------
        # INVENTORY DWELLING SIZE SNAPSHOT
        # Snapshot of dwelling type, dwelling size and PMF
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory_dwelling_size_snapshot (
            inventory_snapshot_id INTEGER NOT NULL,

            dwelling_type TEXT NOT NULL,
            dwelling_size_m2 REAL,
            count_value INTEGER,
            dwelling_type_pmf REAL,

            PRIMARY KEY (inventory_snapshot_id, dwelling_type),

            FOREIGN KEY (inventory_snapshot_id)
                REFERENCES inventory_snapshot(inventory_snapshot_id)
                ON DELETE CASCADE,

            CHECK (dwelling_size_m2 IS NULL OR dwelling_size_m2 >= 0.0),
            CHECK (count_value IS NULL OR count_value >= 0),
            CHECK (dwelling_type_pmf IS NULL OR dwelling_type_pmf >= 0.0)
        );
        """)

        # -------------------------------------------------
        # VIEW: INVENTORY ITEM CARBON LOOKUP
        # Model-facing joined lookup for single-item calculations
        # -------------------------------------------------
        cur.execute("""
        CREATE VIEW IF NOT EXISTS v_inventory_item_carbon_lookup AS
        SELECT
            i.inventory_snapshot_id,

            i.item_name,
            i.item_mass_kg,
            i.furniture_class,

            f.kgC_kg,
            f.ratio_fossil,
            f.ratio_biog,

            i.item_mass_kg * f.kgC_kg
                AS item_total_carbon_kgC,

            i.item_mass_kg * f.kgC_kg * f.ratio_biog
                AS item_biog_carbon_kgC,

            i.item_mass_kg * f.kgC_kg * f.ratio_fossil
                AS item_fossil_carbon_kgC

        FROM inventory_item_snapshot AS i
        LEFT JOIN inventory_furniture_snapshot AS f
            ON i.inventory_snapshot_id = f.inventory_snapshot_id
           AND i.furniture_class = f.furniture_class;
        """)
        

        # Write changes and print confirmation in terminal
        con.commit()
        print(f"Initialised blank fire database at: {db_path}")

    finally:
        con.close()


# Load helper
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="init_fire_db",
        description="Initialise a blank SQLite database for the Fire Emissions fire model.",
    )

    parser.add_argument(
        "--db",
        required=True,
        help="Database handle from config/local_paths.yaml, e.g. fire_db.",
    )

    parser.add_argument(
        "--profile",
        required=True,
        help="Profile name from config/local_paths.yaml, e.g. tom.",
    )

    args = parser.parse_args(argv)

    config = load_local_paths_config(Path("config") / "local_paths.yaml")
    db_path = resolve_db_path(args.profile, args.db, config)

    init_database(str(db_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())