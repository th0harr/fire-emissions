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

    3. input_single_event
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


        # -----------------------
        # FIRE EMISSION PARAMETERS
        # Emission factors for modelling
        # -----------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_emission_parameter_mapping (
            parameter_mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,

            source_id TEXT NOT NULL,

            fire_spread_category TEXT NOT NULL,

            fire_emission_parameter TEXT NOT NULL,

            parameter_type TEXT,
            emission_species TEXT,
            ventilation_condition TEXT,

            is_applicable INTEGER NOT NULL DEFAULT 1,

            value_min REAL,
            value_default REAL,
            value_max REAL,

            notes TEXT,

            source_sheet TEXT NOT NULL,
            source_table TEXT NOT NULL,
            input_row_number INTEGER,

            created_at_utc TEXT NOT NULL,

            FOREIGN KEY (source_id)
                REFERENCES sources(source_id)
                ON DELETE CASCADE,

            CHECK (
                fire_spread_category IN (
                    'single_item',
                    'within_room',
                    'multiple_rooms',
                    'entire_dwelling'
                )
            ),

            CHECK (is_applicable IN (0, 1)),

            CHECK (
                ventilation_condition IS NULL
                OR ventilation_condition IN ('overventilated', 'underventilated')
            ),

            UNIQUE (fire_spread_category, fire_emission_parameter)
        );
        """)


        # -------------------------------------------------
        # SINGLE EVENT INPUT
        # Raw/staging table for one manual fire input workbook
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS input_single_event (
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
            CREATE INDEX IF NOT EXISTS idx_input_single_event_source
            ON input_single_event (source_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_input_single_event_parameter
            ON input_single_event (fire_parameter);
        """)


        # -------------------------------------------------
        # FRIS BULK EVENTS INPUT
        # Raw/staging table for the incident-level FRIS extract.
        #
        # This table is a lightly normalised copy of fris_raw.xlsx:
        #   - original column names are converted to lowercase snake_case
        #   - symbols/parentheses are removed from field names
        #   - all imported rows from one workbook share the same source_id
        #   - incident_id is the FRIS incident identifier and must be unique
        #
        # No model-facing resolution is performed here.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS input_bulk_fris_events (
            source_id TEXT NOT NULL,
            incident_id TEXT NOT NULL PRIMARY KEY,
            fiscal_yr TEXT,
            property_type_3 TEXT,
            heat_smoke_damage_only TEXT,
            ignition_source_all TEXT,
            fire_size_on_arrival TEXT,
            fire_start_location TEXT,
            item_first_ignited TEXT,
            item_causing_spread TEXT,
            extent_of_damage TEXT,
            rapid_fire_growth TEXT,
            building_room_origin_size TEXT,
            building_floor_origin_size TEXT,
            building_fire_damage_area TEXT,
            building_total_damage_area TEXT,
            distance_to_adjoining_property TEXT,

            FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_input_bulk_fris_events_source
            ON input_bulk_fris_events (source_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_input_bulk_fris_events_extent
            ON input_bulk_fris_events (extent_of_damage);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_input_bulk_fris_events_heat_smoke
            ON input_bulk_fris_events (heat_smoke_damage_only);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_input_bulk_fris_events_property_type
            ON input_bulk_fris_events (property_type_3);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_input_bulk_fris_events_fiscal_yr
            ON input_bulk_fris_events (fiscal_yr);
        """)


        # -------------------------------------------------
        # FIRE EVENT MAPPING WARNINGS
        # Controlled warning catalogue for event-resolution assumptions,
        # omissions, and non-blocking data-quality issues.
        #
        # Other fire_event_mapping_* tables store warning_type codes only;
        # warning text and placeholder templates are centralised here.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_event_mapping_warnings (
            warning_type TEXT PRIMARY KEY,

            warning_category TEXT,
            warning_text TEXT NOT NULL,
            notes TEXT,

            mapping_row INTEGER
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_warnings_category
            ON fire_event_mapping_warnings (warning_category);
        """)


        # -------------------------------------------------
        # FIRE EVENT MAPPING: DWELLINGS
        # Mapping from FRIS Property_Type_3 values to model-facing
        # dwelling categories, optional modelling proxies, occupancy flags,
        # and omission/warning decisions.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_event_mapping_dwellings (
            mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,

            mapping_row INTEGER,

            fris_dwelling_naming TEXT NOT NULL,
            dwelling_type TEXT,
            dwelling_type_proxy TEXT,
            occupancy_override TEXT,

            omit_from_model INTEGER NOT NULL DEFAULT 0,
            warning_type TEXT,
            notes TEXT,

            CHECK (omit_from_model IN (0, 1)),
            CHECK (
                occupancy_override IS NULL
                OR occupancy_override IN ('single', 'multiple', 'unknown')
            ),

            UNIQUE (fris_dwelling_naming)
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_dwellings_type
            ON fire_event_mapping_dwellings (dwelling_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_dwellings_proxy
            ON fire_event_mapping_dwellings (dwelling_type_proxy);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_dwellings_warning
            ON fire_event_mapping_dwellings (warning_type);
        """)


        # -------------------------------------------------
        # FIRE EVENT MAPPING: FIRE CATEGORIES
        # Mapping from FRIS Extent_of_Damage values to canonical
        # fire_spread_category values, including omission flags and
        # occupancy-dependent interpretation flags.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_event_mapping_fire_cat (
            mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,

            mapping_row INTEGER,

            fris_fire_categories TEXT NOT NULL,
            fire_spread_category TEXT NOT NULL,

            omit_from_model INTEGER NOT NULL DEFAULT 0,
            occupancy_dependent INTEGER NOT NULL DEFAULT 0,

            warning_type TEXT,
            conditional_warning INTEGER NOT NULL DEFAULT 0,
            notes TEXT,

            CHECK (
                fire_spread_category IN (
                    'none',
                    'heat_smoke_damage_only',
                    'single_item',
                    'within_room',
                    'multiple_rooms',
                    'entire_dwelling',
                    'roof',
                    'unspecified'
                )
            ),
            CHECK (omit_from_model IN (0, 1)),
            CHECK (occupancy_dependent IN (0, 1)),
            CHECK (conditional_warning IN (0, 1)),

            UNIQUE (fris_fire_categories)
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_fire_cat_spread
            ON fire_event_mapping_fire_cat (fire_spread_category);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_fire_cat_warning
            ON fire_event_mapping_fire_cat (warning_type);
        """)


        # -------------------------------------------------
        # FIRE EVENT MAPPING: ITEMS
        # Mapping from FRIS Ignition_Source_All values to inventory items
        # for single-item fire modelling.
        #
        # ignition_source_category_override / ignition_source_override are
        # only used where the parsed Ignition_Source_All components need to
        # be replaced by model-facing alternatives.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_event_mapping_items (
            mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,

            mapping_row INTEGER,

            ignition_source_all TEXT NOT NULL,
            ignition_source_category_override TEXT,
            ignition_source_override TEXT,

            single_item_status TEXT NOT NULL DEFAULT 'invalid_single_item',
            item_combusted TEXT,

            warning_type TEXT,
            notes TEXT,

            CHECK (
                single_item_status IN (
                    'direct_inventory_item',
                    'proxy_inventory_item',
                    'conditionally_inferred_item',
                    'invalid_single_item'
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
                    single_item_status IN ('conditionally_inferred_item', 'invalid_single_item')
                    AND (
                        item_combusted IS NULL
                        OR TRIM(item_combusted) = ''
                    )
                )
            ),

            UNIQUE (ignition_source_all)
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_items_status
            ON fire_event_mapping_items (single_item_status);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_items_item
            ON fire_event_mapping_items (item_combusted);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_items_warning
            ON fire_event_mapping_items (warning_type);
        """)


        # -------------------------------------------------
        # FIRE EVENT MAPPING: CONDITIONAL ITEM INFERENCE
        # Row-wise contextual proxy rules used when:
        #   fire_event_mapping_items.single_item_status =
        #   'conditionally_inferred_item'
        #
        # These rules are intended for single-item cases where the ignition
        # source alone is insufficient, but room_type and/or item_first_ignited
        # can support a defensible proxy item.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_event_mapping_item_inference (
            inference_id INTEGER PRIMARY KEY AUTOINCREMENT,

            mapping_row INTEGER,

            ignition_category TEXT,
            ignition_source TEXT NOT NULL,
            fire_spread_category TEXT NOT NULL,
            room_type TEXT,
            item_first_ignited TEXT,
            item_combusted TEXT NOT NULL,
            notes TEXT,

            CHECK (fire_spread_category = 'single_item'),
            CHECK (TRIM(item_combusted) <> ''),

            UNIQUE (
                ignition_category,
                ignition_source,
                fire_spread_category,
                room_type,
                item_first_ignited
            )
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_item_inference_lookup
            ON fire_event_mapping_item_inference (
                ignition_category,
                ignition_source,
                fire_spread_category,
                room_type,
                item_first_ignited
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_item_inference_item
            ON fire_event_mapping_item_inference (item_combusted);
        """)


        # -------------------------------------------------
        # FIRE EVENT MAPPING: ROOMS
        # Mapping from FRIS Fire_Start_Location values to model-facing
        # room_type values. Inclusion/omission is decided downstream from the
        # resolved fire_spread_category; this table only resolves the room.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_event_mapping_rooms (
            mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,

            mapping_row INTEGER,

            fire_start_location TEXT NOT NULL,
            room_type TEXT,
            warning_type TEXT,
            notes TEXT,

            UNIQUE (fire_start_location)
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_rooms_room
            ON fire_event_mapping_rooms (room_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_rooms_warning
            ON fire_event_mapping_rooms (warning_type);
        """)


        # -------------------------------------------------
        # FIRE EVENT MAPPING: AREA BANDS
        # Controlled ordering for FRIS damage-area bands. Used for comparing
        # building_fire_damage_area and building_total_damage_area bands.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_event_mapping_area_bands (
            mapping_id INTEGER PRIMARY KEY AUTOINCREMENT,

            mapping_row INTEGER,

            area_band TEXT NOT NULL,
            band_order INTEGER NOT NULL,
            is_none_band INTEGER NOT NULL DEFAULT 0,
            is_open_ended INTEGER NOT NULL DEFAULT 0,
            notes TEXT,

            CHECK (band_order >= 0),
            CHECK (is_none_band IN (0, 1)),
            CHECK (is_open_ended IN (0, 1)),

            UNIQUE (area_band),
            UNIQUE (band_order)
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_mapping_area_bands_order
            ON fire_event_mapping_area_bands (band_order);
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
            expected_embodied_CO2_kg REAL,

            q25_total_carbon_kgC REAL,
            q25_biog_carbon_kgC REAL,
            q25_fossil_carbon_kgC REAL,
            q25_embodied_CO2_kg REAL,

            q75_total_carbon_kgC REAL,
            q75_biog_carbon_kgC REAL,
            q75_fossil_carbon_kgC REAL,
            q75_embodied_CO2_kg REAL,
           
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
            CHECK (expected_embodied_CO2_kg IS NULL OR expected_embodied_CO2_kg >= 0.0),

            CHECK (q25_total_carbon_kgC IS NULL OR q25_total_carbon_kgC >= 0.0),
            CHECK (q25_biog_carbon_kgC IS NULL OR q25_biog_carbon_kgC >= 0.0),
            CHECK (q25_fossil_carbon_kgC IS NULL OR q25_fossil_carbon_kgC >= 0.0),
            CHECK (q25_embodied_CO2_kg IS NULL OR q25_embodied_CO2_kg >= 0.0),

            CHECK (q75_total_carbon_kgC IS NULL OR q75_total_carbon_kgC >= 0.0),
            CHECK (q75_biog_carbon_kgC IS NULL OR q75_biog_carbon_kgC >= 0.0),
            CHECK (q75_fossil_carbon_kgC IS NULL OR q75_fossil_carbon_kgC >= 0.0),
            CHECK (q75_embodied_CO2_kg IS NULL OR q75_embodied_CO2_kg >= 0.0)
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
        # FIRE EVENTS
        # Resolved/model-facing fire event records.
        #
        # One row represents one fire event / one input case.
        #
        # This table is built primarily from:
        #   input_bulk_fris_events
        #   fire_event_mapping_* tables
        #   inventory_*_snapshot tables
        #
        # Future scenario / single-event routes should also resolve into this
        # same table shape, but FRIS is the primary route for now.
        #
        # Key design choice:
        #   event_id is the internal model-facing primary key.
        #   incident_id stores the original/source event identifier, e.g. FRIS
        #   Incident_Id or a future synthetic scenario identifier.
        #   source_id links back to the imported source workbook/file.
        #
        # Omitted/invalid events are retained with omit_from_model = 1 so that
        # model coverage, omission reasons and future inclusion opportunities
        # can be reported transparently.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_events (

            source_id TEXT NOT NULL,
            incident_id TEXT NOT NULL,
            input_type TEXT NOT NULL,

            fiscal_year_start INTEGER,
            fiscal_year_end INTEGER,

            property_type_3_input TEXT,
            dwelling_type TEXT,
            dwelling_type_proxy TEXT,
            dwelling_type_for_model TEXT,
            occupancy TEXT,

            heat_smoke_damage_only_input TEXT,
            extent_of_damage_input TEXT,
            fire_spread_category_from_extent TEXT,
            fire_spread_category TEXT NOT NULL,

            fire_start_location_input TEXT,
            room_of_origin TEXT,

            building_fire_damage_area_input TEXT,
            building_fire_damage_area_band_index INTEGER,
            building_total_damage_area_input TEXT,
            building_total_damage_area_band_index INTEGER,
            building_room_origin_size_input TEXT,
            building_room_origin_size_band_index INTEGER,
            building_floor_origin_size_input TEXT,

            ignition_source_all_input TEXT,
            ignition_source_category_input TEXT,
            ignition_source_input TEXT,
            ignition_source_category TEXT,
            ignition_source TEXT,

            item_first_ignited_input TEXT,
            item_causing_spread_input TEXT,

            single_item_status TEXT,
            item_combusted TEXT,

            omit_from_model TEXT NOT NULL DEFAULT 'no',
            omit_reason TEXT,
            data_quality_status TEXT NOT NULL DEFAULT 'ok',
            suspicious_fields TEXT,

            resolution_notes TEXT,

            FOREIGN KEY (source_id)
                REFERENCES sources(source_id)
                ON DELETE CASCADE,

            UNIQUE (input_type, incident_id),

            CHECK (
                input_type IN (
                    'fris',
                    'scenario',
                    'single_legacy'
                )
            ),

            CHECK (
                occupancy IS NULL
                OR occupancy IN (
                    'single',
                    'multiple',
                    'unknown'
                )
            ),

            CHECK (
                fire_spread_category_from_extent IS NULL
                OR fire_spread_category_from_extent IN (
                    'none',
                    'heat_smoke',
                    'heat_smoke_damage_only',
                    'single_item',
                    'within_room',
                    'multiple_rooms',
                    'entire_dwelling',
                    'roof',
                    'unspecified'
                )
            ),

            CHECK (
                fire_spread_category IN (
                    'none',
                    'heat_smoke',
                    'heat_smoke_damage_only',
                    'single_item',
                    'within_room',
                    'multiple_rooms',
                    'entire_dwelling',
                    'roof',
                    'unspecified'
                )
            ),

            CHECK (
                single_item_status IS NULL
                OR single_item_status IN (
                    'direct_inventory_item',
                    'proxy_inventory_item',
                    'conditionally_inferred_item',
                    'invalid_single_item'
                )
            ),

            CHECK (
                omit_from_model IN ('yes', 'no')
            ),

            CHECK (
                data_quality_status IN (
                    'ok',
                    'warning',
                    'omit'
                )
            ),

            CHECK (
                building_fire_damage_area_band_index IS NULL
                OR building_fire_damage_area_band_index >= 0
            ),

            CHECK (
                building_total_damage_area_band_index IS NULL
                OR building_total_damage_area_band_index >= 0
            )
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_events_source
            ON fire_events (source_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_events_incident
            ON fire_events (incident_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_events_input_type
            ON fire_events (input_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_events_spread
            ON fire_events (fire_spread_category);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_events_extent_spread
            ON fire_events (fire_spread_category_from_extent);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_events_room
            ON fire_events (room_of_origin);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_events_dwelling
            ON fire_events (dwelling_type_for_model);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_events_item_combusted
            ON fire_events (item_combusted);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_events_omit
            ON fire_events (omit_from_model, omit_reason);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_events_quality
            ON fire_events (data_quality_status);
        """)

        # -------------------------------------------------
        # FIRE EVENT WARNINGS
        # Structured warnings generated during fire-event resolution.
        #
        # One fire event may have zero, one or many warning rows.
        #
        # event_id links each warning to the resolved/model-facing event row.
        # incident_id is also retained for easier inspection/export, but event_id
        # is the formal relationship to fire_events.
        #
        # These warnings should be deleted before fire_events when --overwrite is
        # used, because they depend on rows in fire_events.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_event_warnings (
            warning_id INTEGER PRIMARY KEY AUTOINCREMENT,

            source_id TEXT,
            incident_id TEXT NOT NULL,
            input_type TEXT NOT NULL,

            warning_category TEXT,
            warning_type TEXT NOT NULL,
            warning_severity TEXT NOT NULL DEFAULT 'warning',
            warning_text TEXT NOT NULL,

            fire_parameter TEXT,
            raw_value TEXT,
            resolved_value TEXT,
            created_at_utc TEXT,

            CHECK (
                warning_severity IN (
                    'info',
                    'warning',
                    'omit_row',
                    'blocking'
                )
            )
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_warnings_incident
            ON fire_event_warnings (input_type, incident_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_warnings_type
            ON fire_event_warnings (warning_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_warnings_severity
            ON fire_event_warnings (warning_severity);
        """)


        # -------------------------------------------------
        # FIRE OMISSION SUMMARY
        # Record of omission reapons used during fire-event resolution.
        # -------------------------------------------------

        cur.execute("""
            CREATE TABLE IF NOT EXISTS fire_event_omission_summary (
                summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
                input_type TEXT NOT NULL,
                omit_reason TEXT NOT NULL,
                omitted_count INTEGER NOT NULL CHECK (omitted_count >= 0),
                created_at_utc TEXT NOT NULL DEFAULT (
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                ),
                UNIQUE (input_type, omit_reason)
            );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_omission_summary_input_type
            ON fire_event_omission_summary (input_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_event_omission_summary_count
            ON fire_event_omission_summary (omitted_count DESC);
        """)


        # -------------------------------------------------
        # MODEL OMISSION SUMMARY
        # Record of omission reapons used during emissions modelling.
        # -------------------------------------------------

        cur.execute("""
            CREATE TABLE IF NOT EXISTS fire_model_omission_summary (
                summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_name TEXT NOT NULL,
                model_version TEXT NOT NULL,
                input_type TEXT NOT NULL,
                omit_reason TEXT NOT NULL,
                omitted_count INTEGER NOT NULL CHECK (omitted_count >= 0),
                created_at_utc TEXT NOT NULL DEFAULT (
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                ),
                UNIQUE (model_name, input_type, omit_reason)
            );
        """)        
         

        # -------------------------------------------------
        # FIRE MODEL METADATA
        # Latest deterministic fire-emissions model build metadata.
        #
        # This is deliberately a single-row table for the current project stage:
        #   - the Stage 1 / Stage 2 model output tables are rebuilt/overwritten
        #     during development
        #   - full model-run history can be added later if needed
        #
        # metadata_id is constrained to 1 so this table stores only the latest
        # model build metadata.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_model_metadata (
            metadata_id INTEGER PRIMARY KEY CHECK (metadata_id = 1),

            model_name TEXT NOT NULL DEFAULT 'fire_emissions',
            model_version TEXT,
            model_description TEXT,

            input_type TEXT,
            event_rows_read INTEGER,
            event_rows_modelled INTEGER,
            event_rows_omitted INTEGER,

            emission_parameter_source_id TEXT,
            emission_parameter_rows INTEGER,

            inventory_snapshot_id INTEGER,

            estimate_cases_built TEXT,
            include_area_range INTEGER,
            include_stock_range INTEGER,
            include_emission_parameter_range INTEGER,

            started_utc TEXT,
            finished_utc TEXT,
            created_at_utc TEXT NOT NULL,

            rows_stage1_written INTEGER,
            rows_stage2_written INTEGER,
            rows_warnings_written INTEGER,

            notes TEXT,

            CHECK (event_rows_read IS NULL OR event_rows_read >= 0),
            CHECK (event_rows_modelled IS NULL OR event_rows_modelled >= 0),
            CHECK (event_rows_omitted IS NULL OR event_rows_omitted >= 0),
            CHECK (emission_parameter_rows IS NULL OR emission_parameter_rows >= 0),
            CHECK (rows_stage1_written IS NULL OR rows_stage1_written >= 0),
            CHECK (rows_stage2_written IS NULL OR rows_stage2_written >= 0),
            CHECK (rows_warnings_written IS NULL OR rows_warnings_written >= 0),

            CHECK (
                include_area_range IS NULL
                OR include_area_range IN (0, 1)
            ),

            CHECK (
                include_stock_range IS NULL
                OR include_stock_range IN (0, 1)
            ),

            CHECK (
                include_emission_parameter_range IS NULL
                OR include_emission_parameter_range IN (0, 1)
            ),

            FOREIGN KEY (emission_parameter_source_id)
                REFERENCES sources(source_id)
                ON DELETE SET NULL,

            FOREIGN KEY (inventory_snapshot_id)
                REFERENCES inventory_snapshot(inventory_snapshot_id)
                ON DELETE SET NULL
        );
        """)


        # -------------------------------------------------
        # FIRE MODEL STAGE 1 COMPONENT RESULTS
        # Deterministic affected-stock and replacement/embodied component table.
        #
        # One row represents one resolved fire event component for one estimate
        # case, before gas-species emission factors are applied.
        #
        # Stage 1 keeps two pathways separate:
        #   1. direct affected carbon stock, kgC
        #   2. replacement / embodied CO2, kg CO2
        #
        # This table is intended to be rebuilt/overwritten during the current
        # development stage. It is still event/component-level so later summary
        # tables, scenarios, filters, plots, or reports can be derived from it
        # without rerunning all earlier resolution logic.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_model_stage1_component_results (
            stage1_result_id INTEGER PRIMARY KEY AUTOINCREMENT,

            source_id TEXT,
            incident_id TEXT,
            input_type TEXT,
            inventory_snapshot_id INTEGER,

            estimate_case TEXT NOT NULL,

            fiscal_year_start INTEGER,
            fiscal_year_end INTEGER,

            property_type_3_input TEXT,
            dwelling_type TEXT,
            dwelling_type_proxy TEXT,
            dwelling_type_for_model TEXT,
            occupancy TEXT,

            affected_dwelling_count INTEGER NOT NULL DEFAULT 1,

            fire_spread_category TEXT NOT NULL,
            fire_spread_category_from_extent TEXT,

            room_of_origin TEXT,
            room_of_origin_proxy TEXT,

            single_item_status TEXT,
            item_combusted TEXT,

            component_type TEXT NOT NULL,
            emission_pathway TEXT NOT NULL,

            area_basis TEXT,
            stock_basis TEXT,
            embodied_basis TEXT,

            building_fire_damage_area_input TEXT,
            building_fire_damage_area_band_index INTEGER,
            building_fire_damage_area_m2 REAL,  

            building_total_damage_area_input TEXT,
            building_total_damage_area_for_model TEXT,
            building_total_damage_area_band_index INTEGER,
            building_total_damage_area_m2 REAL,

            room_of_origin_size_m2 REAL,
            dwelling_size_m2 REAL,

            residual_fire_area_m2 REAL    
            residual_dwelling_area_m2 REAL
            replacement_damage_area_m2 REAL                

            area_fraction REAL,
            room_fire_fraction REAL,
            room_damage_fraction REAL,
            residual_fire_fraction REAL,
            dwelling_damage_fraction REAL,

            direct_total_kgC REAL,
            direct_biogenic_kgC REAL,
            direct_fossil_kgC REAL,

            replacement_embodied_CO2_kg REAL,

            calculation_status TEXT NOT NULL DEFAULT 'ok',
            calculation_notes TEXT,

            created_at_utc TEXT NOT NULL,

            FOREIGN KEY (inventory_snapshot_id)
                REFERENCES inventory_snapshot(inventory_snapshot_id)
                ON DELETE SET NULL,

            CHECK (
                input_type IS NULL
                OR input_type IN (
                    'fris',
                    'scenario',
                    'single_legacy'
                )
            ),

            CHECK (
                estimate_case IN (
                    'low',
                    'default',
                    'high'
                )
            ),

            CHECK (
                occupancy IS NULL
                OR occupancy IN (
                    'single',
                    'multiple',
                    'unknown'
                )
            ),

            CHECK (
                fire_spread_category IN (
                    'none',
                    'heat_smoke_damage_only',
                    'single_item',
                    'within_room',
                    'multiple_rooms',
                    'entire_dwelling',
                    'roof',
                    'unspecified'
                )
            ),

            CHECK (
                fire_spread_category_from_extent IS NULL
                OR fire_spread_category_from_extent IN (
                    'none',
                    'heat_smoke_damage_only',
                    'single_item',
                    'within_room',
                    'multiple_rooms',
                    'entire_dwelling',
                    'roof',
                    'unspecified'
                )
            ),

            CHECK (
                single_item_status IS NULL
                OR single_item_status IN (
                    'direct_inventory_item',
                    'proxy_inventory_item',
                    'conditionally_inferred_item',
                    'invalid_single_item'
                )
            ),

            CHECK (
                emission_pathway IN (
                    'direct',
                    'replacement',
                    'direct_and_replacement',
                    'none'
                )
            ),

            CHECK (
                calculation_status IN (
                    'ok',
                    'warning',
                    'omitted',
                    'error'
                )
            ),

            CHECK (building_fire_damage_area_band_index IS NULL OR building_fire_damage_area_band_index >= 0),
            CHECK (building_total_damage_area_band_index IS NULL OR building_total_damage_area_band_index >= 0),

            CHECK (building_fire_damage_area_m2 IS NULL OR building_fire_damage_area_m2 >= 0.0),
            CHECK (building_total_damage_area_m2 IS NULL OR building_total_damage_area_m2 >= 0.0),

            CHECK (affected_dwelling_count >= 1),

            CHECK (room_of_origin_size_m2 IS NULL OR room_of_origin_size_m2 >= 0.0),
            CHECK (dwelling_size_m2 IS NULL OR dwelling_size_m2 >= 0.0),

            CHECK (residual_fire_area_m2 IS NULL OR residual_fire_area_m2 >= 0),
            CHECK (residual_dwelling_area_m2 IS NULL OR residual_dwelling_area_m2 >= 0),
            CHECK (replacement_damage_area_m2 IS NULL OR replacement_damage_area_m2 >= 0),

            CHECK (area_fraction IS NULL OR area_fraction >= 0.0),
            CHECK (room_fire_fraction IS NULL OR room_fire_fraction >= 0.0),
            CHECK (room_damage_fraction IS NULL OR room_damage_fraction >= 0.0),
            CHECK (residual_fire_fraction IS NULL OR residual_fire_fraction >= 0.0),
            CHECK (dwelling_damage_fraction IS NULL OR dwelling_damage_fraction >= 0.0),

            CHECK (direct_total_kgC IS NULL OR direct_total_kgC >= 0.0),
            CHECK (direct_biogenic_kgC IS NULL OR direct_biogenic_kgC >= 0.0),
            CHECK (direct_fossil_kgC IS NULL OR direct_fossil_kgC >= 0.0),
            CHECK (replacement_embodied_CO2_kg IS NULL OR replacement_embodied_CO2_kg >= 0.0)
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage1_source
            ON fire_model_stage1_component_results (source_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage1_incident
            ON fire_model_stage1_component_results (incident_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage1_input_type
            ON fire_model_stage1_component_results (input_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage1_estimate_case
            ON fire_model_stage1_component_results (estimate_case);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage1_spread
            ON fire_model_stage1_component_results (fire_spread_category);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage1_dwelling
            ON fire_model_stage1_component_results (dwelling_type_for_model);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage1_room
            ON fire_model_stage1_component_results (room_of_origin);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage1_component
            ON fire_model_stage1_component_results (component_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage1_pathway
            ON fire_model_stage1_component_results (emission_pathway);
        """)


        # -------------------------------------------------
        # FIRE MODEL STAGE 2 SPECIES RESULTS
        # Deterministic direct gas-species emissions table.
        #
        # One row represents one emitted species / carbon-origin result derived
        # from one Stage 1 component and one estimate case.
        #
        # Stage 2 applies:
        #   - combustion completeness
        #   - neutral char formation factor
        #   - blended species emission factor
        #   - molecular conversion factor
        #
        # This table is generic over emission_species so that later species can
        # be added through fire_emission_parameter_mapping without changing the
        # output schema.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_model_stage2_species_results (
            stage2_result_id INTEGER PRIMARY KEY AUTOINCREMENT,

            stage1_result_id INTEGER,

            source_id TEXT,
            incident_id TEXT,
            input_type TEXT,
            inventory_snapshot_id INTEGER,

            estimate_case TEXT NOT NULL,

            fiscal_year_start INTEGER,
            fiscal_year_end INTEGER,

            property_type_3_input TEXT,
            dwelling_type TEXT,
            dwelling_type_proxy TEXT,
            dwelling_type_for_model TEXT,
            occupancy TEXT,

            fire_spread_category TEXT NOT NULL,
            fire_spread_category_from_extent TEXT,

            room_of_origin TEXT,
            room_of_origin_proxy TEXT,

            component_type TEXT NOT NULL,

            emission_species TEXT NOT NULL,
            carbon_origin TEXT NOT NULL,

            parameter_fire_spread_category TEXT,
            species_factor_parameter_name TEXT,

            ventilation_condition_case TEXT,
            fire_development_case TEXT,

            direct_affected_kgC REAL,
            combusted_kgC REAL,

            combustion_completeness_factor REAL,
            char_formation_factor REAL,
            post_flashover_weighting REAL,

            species_emission_factor REAL,
            molecular_conversion_factor REAL,

            emitted_kg REAL,

            carbon_partition_sum REAL,

            calculation_status TEXT NOT NULL DEFAULT 'ok',
            calculation_notes TEXT,

            created_at_utc TEXT NOT NULL,

            FOREIGN KEY (stage1_result_id)
                REFERENCES fire_model_stage1_component_results(stage1_result_id)
                ON DELETE CASCADE,

            FOREIGN KEY (inventory_snapshot_id)
                REFERENCES inventory_snapshot(inventory_snapshot_id)
                ON DELETE SET NULL,

            CHECK (
                input_type IS NULL
                OR input_type IN (
                    'fris',
                    'scenario',
                    'single_legacy'
                )
            ),

            CHECK (
                estimate_case IN (
                    'low',
                    'default',
                    'high'
                )
            ),

            CHECK (
                occupancy IS NULL
                OR occupancy IN (
                    'single',
                    'multiple',
                    'unknown'
                )
            ),

            CHECK (
                fire_spread_category IN (
                    'none',
                    'heat_smoke_damage_only',
                    'single_item',
                    'within_room',
                    'multiple_rooms',
                    'entire_dwelling',
                    'roof',
                    'unspecified'
                )
            ),

            CHECK (
                fire_spread_category_from_extent IS NULL
                OR fire_spread_category_from_extent IN (
                    'none',
                    'heat_smoke_damage_only',
                    'single_item',
                    'within_room',
                    'multiple_rooms',
                    'entire_dwelling',
                    'roof',
                    'unspecified'
                )
            ),

            CHECK (
                parameter_fire_spread_category IS NULL
                OR parameter_fire_spread_category IN (
                    'single_item',
                    'within_room',
                    'multiple_rooms',
                    'entire_dwelling'
                )
            ),

            CHECK (
                carbon_origin IN (
                    'biogenic',
                    'fossil',
                    'total'
                )
            ),

            CHECK (
                ventilation_condition_case IS NULL
                OR ventilation_condition_case IN (
                    'overventilated',
                    'underventilated',
                    'blended',
                    'not_applicable'
                )
            ),

            CHECK (
                calculation_status IN (
                    'ok',
                    'warning',
                    'omitted',
                    'error'
                )
            ),

            CHECK (direct_affected_kgC IS NULL OR direct_affected_kgC >= 0.0),
            CHECK (combusted_kgC IS NULL OR combusted_kgC >= 0.0),

            CHECK (
                combustion_completeness_factor IS NULL
                OR (
                    combustion_completeness_factor >= 0.0
                    AND combustion_completeness_factor <= 1.0
                )
            ),

            CHECK (
                char_formation_factor IS NULL
                OR (
                    char_formation_factor >= 0.0
                    AND char_formation_factor <= 1.0
                )
            ),

            CHECK (
                post_flashover_weighting IS NULL
                OR (
                    post_flashover_weighting >= 0.0
                    AND post_flashover_weighting <= 1.0
                )
            ),

            CHECK (
                species_emission_factor IS NULL
                OR (
                    species_emission_factor >= 0.0
                    AND species_emission_factor <= 1.0
                )
            ),

            CHECK (molecular_conversion_factor IS NULL OR molecular_conversion_factor >= 0.0),
            CHECK (emitted_kg IS NULL OR emitted_kg >= 0.0),

            CHECK (
                carbon_partition_sum IS NULL
                OR (
                    carbon_partition_sum >= 0.0
                    AND carbon_partition_sum <= 1.0000001
                )
            )
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage2_stage1
            ON fire_model_stage2_species_results (stage1_result_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage2_source
            ON fire_model_stage2_species_results (source_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage2_incident
            ON fire_model_stage2_species_results (incident_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage2_input_type
            ON fire_model_stage2_species_results (input_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage2_estimate_case
            ON fire_model_stage2_species_results (estimate_case);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage2_spread
            ON fire_model_stage2_species_results (fire_spread_category);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage2_dwelling
            ON fire_model_stage2_species_results (dwelling_type_for_model);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage2_room
            ON fire_model_stage2_species_results (room_of_origin);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage2_component
            ON fire_model_stage2_species_results (component_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage2_species
            ON fire_model_stage2_species_results (emission_species);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_stage2_carbon_origin
            ON fire_model_stage2_species_results (carbon_origin);
        """)


        # -------------------------------------------------
        # FIRE MODEL WARNINGS
        # Calculation-stage warnings for the deterministic fire-emissions model.
        #
        # These are separate from fire_event_warnings:
        #   - fire_event_warnings are produced during fire-event resolution
        #   - fire_model_warnings are produced during area conversion, stock
        #     lookup, Stage 1 affected-stock modelling, Stage 2 gas-species
        #     emissions, and later reporting
        #
        # This table is rebuilt/overwritten with each current fire-emissions
        # model build.
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS fire_model_warnings (
            model_warning_id INTEGER PRIMARY KEY AUTOINCREMENT,

            source_id TEXT,
            incident_id TEXT,
            input_type TEXT,

            stage TEXT NOT NULL,

            warning_type TEXT NOT NULL,
            warning_severity TEXT,
            warning_category TEXT,
            warning_text TEXT NOT NULL,

            fire_parameter TEXT,
            raw_value TEXT,
            resolved_value TEXT,

            created_at_utc TEXT NOT NULL,

            CHECK (
                input_type IS NULL
                OR input_type IN (
                    'fris',
                    'scenario',
                    'single_legacy'
                )
            ),

            CHECK (
                warning_severity IS NULL
                OR warning_severity IN (
                    'info',
                    'warning',
                    'model_assumption',
                    'omit_row',
                    'blocking',
                    'error'
                )
            )
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_warnings_source
            ON fire_model_warnings (source_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_warnings_incident
            ON fire_model_warnings (incident_id);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_warnings_input_type
            ON fire_model_warnings (input_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_warnings_stage
            ON fire_model_warnings (stage);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_warnings_type
            ON fire_model_warnings (warning_type);
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_fire_model_warnings_severity
            ON fire_model_warnings (warning_severity);
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