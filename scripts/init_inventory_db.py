"""
Initialise a blank SQLite database for the Fire Emissions inventory project.
Note: this only provides the database structue, it will not add items (ingest data)

Run from the project root:
    python scripts/init_db.py
"""

import argparse
import sqlite3
from pathlib import Path

from scripts.ingest import load_local_paths_config

# FUNCTION: Create the intended DB filepath
def resolve_db_path(profile: str, db_handle: str, config: dict) -> Path:
    """
    Resolve the full SQLite database path for the selected profile and db handle
    from local_paths.yaml"
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


# FUNCTION: creates a blank SQLite database; taking a string as input and returning nothing
def init_database(sqlite_path: str) -> None:
    db_path = Path(sqlite_path)                         # converts input string into sqlite path
    db_path.parent.mkdir(parents=True, exist_ok=True)   # creates subfolders if included in str

    # Opens connection to database file OR creates it
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()

        # Enable defaults (PRAGMAs = SQLite settings)
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute("PRAGMA journal_mode = WAL;")

        # -----------------------------------------
        # SOURCES
        # Build sources data table headings & type
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

        # Speed up queries based on data_source_type
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_sources_data_source_type
            ON sources (data_source_type);
        """)

                # ----------------------------------------------
        # INVENTORY OBSERVATIONS
        # Build inventory data table headings & type
        # Note: Can delete references based on source_id
        # ----------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory_observations (
            obs_id INTEGER PRIMARY KEY,
            response_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            room_type TEXT,
            item_name TEXT NOT NULL,
            count INTEGER NOT NULL,
            FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE,
            FOREIGN KEY (room_type) REFERENCES room(room_type),
            FOREIGN KEY (item_name) REFERENCES item_dictionary(item_name)
        );
        """)

        # Speed up queries based on commonly used variables
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_inv_source
            ON inventory_observations (source_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_inv_response_id
            ON inventory_observations (response_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_inv_room
            ON inventory_observations (room_type);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_inv_item_name
            ON inventory_observations (item_name);
        """)

        # -------------------------------------------------
        # DWELLING OBSERVATIONS
        # Build dwelling data table headings & type
        # Note: Can delete references based on source_id
        # -------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS dwelling_observations (
            dwelling_id INTEGER PRIMARY KEY,
            response_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            room_type TEXT NOT NULL,
            count INTEGER NOT NULL,
            assumption_notes TEXT,
            FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE,
            FOREIGN KEY (room_type) REFERENCES room(room_type)
        );
        """)

        # Speed up queries based on commonly used variables
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dwell_source
            ON dwelling_observations (source_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dwell_response_id
            ON dwelling_observations (response_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dwell_room
            ON dwelling_observations (room_type);
        """)

        # -------------------------------------
        # SURVEY COMMENTS
        # Build survey comments table headings & type
        # Note: Can delete references based on source_id
        # -------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS survey_comments (
            comment_obs_id INTEGER PRIMARY KEY,
            response_id TEXT NOT NULL,
            source_id TEXT NOT NULL,
            comment_type TEXT NOT NULL,
            comment_text TEXT NOT NULL,
            FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE
        );
        """)

        # Speed up queries based on commonly used variables
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_comments_source
            ON survey_comments (source_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_comments_response_id
            ON survey_comments (response_id);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_comments_type
            ON survey_comments (comment_type);
        """)

# -------------------------------------
        # ITEM DICTIONARY (curated vocab)
        # Controlled vocabulary / mapping table
        # Contains the canonical list of items
        # From mapping_list.xlsx sheet: "item_name"
        # -------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS item_dictionary (
            item_name TEXT PRIMARY KEY,
            item_description TEXT NOT NULL UNIQUE,
            item_mass REAL,
            furniture_class TEXT,
            notes TEXT,
            FOREIGN KEY (furniture_class) REFERENCES furniture(furniture_class)
        );
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_item_dict_furniture_class
            ON item_dictionary (furniture_class);
        """)

        # -------------------------------------
        # FURNITURE (curated vocab)
        # Stores category level data
        # From mapping_list.xlsx sheet: "furniture_class"
        # -------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS furniture (
            furniture_class TEXT PRIMARY KEY,
            furniture_description TEXT,
            class_contains TEXT,
            kgC_kg REAL,
            ratio_fossil REAL,
            ratio_biog REAL,
            notes TEXT
        );
        """)

        # -------------------------------------
        # ROOM (curated vocab)
        # From mapping_list.xlsx sheet: "room_type"
        # -------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS room (
            room_type TEXT PRIMARY KEY,
            room_description TEXT NOT NULL UNIQUE,
            room_size REAL NOT NULL,
            size_assumed INTEGER,
            assumption_notes TEXT,
            notes TEXT
        );
        """)


        # -------------------------------------
        # ASSUMED INVENTORY
        # Build assumed inventory table headings & type
        # -------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS assumed_inventory (
            assumed_item_id INTEGER PRIMARY KEY,
            room_type TEXT NOT NULL,
            item_name TEXT NOT NULL,
            count_assumed INTEGER NOT NULL,
            assumption_notes TEXT,
            FOREIGN KEY (room_type) REFERENCES room(room_type),
            FOREIGN KEY (item_name) REFERENCES item_dictionary(item_name)
        );
        """)

        # Speed up queries based on commonly used variables
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_assumed_room
            ON assumed_inventory (room_type);
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_assumed_item_name
            ON assumed_inventory (item_name);
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
        # ITEM COUNT DISTRIBUTION
        # Count probability distribution
        # -----------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS item_count_pmf (
            item_pmf_id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_name TEXT NOT NULL,
            room_type TEXT NOT NULL,
            count_value INTEGER NOT NULL,
            item_probability REAL NOT NULL,
            item_pmf_notes TEXT,
            FOREIGN KEY (item_name) REFERENCES item_dictionary(item_name),
            FOREIGN KEY (room_type) REFERENCES room(room_type),
            CHECK (count_value >= 0),
            CHECK (item_probability >= 0.0 AND item_probability <= 1.0),
            UNIQUE (item_name, room_type, count_value)
            )
        """)

        # Speed up queries based on commonly used variables
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_item_count_pmf_item_room
            ON item_count_pmf (item_name, room_type)
        """)

        # -----------------------
        # ITEM COUNT SUMMARY
        # Count probality summary
        # -----------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS item_count_summary (
            count_summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_name TEXT NOT NULL,
            room_type TEXT NOT NULL,
            expected_count_mean REAL NOT NULL,
            count_ci_lower REAL,
            count_ci_upper REAL,
            n_observations INTEGER,
            count_summary_notes TEXT,
            FOREIGN KEY (item_name) REFERENCES item_dictionary(item_name),
            FOREIGN KEY (room_type) REFERENCES room(room_type),
            CHECK (expected_count_mean >= 0.0),
            CHECK (count_ci_lower IS NULL OR count_ci_lower >= 0.0),
            CHECK (count_ci_upper IS NULL OR count_ci_upper >= 0.0),
            CHECK (n_observations IS NULL OR n_observations >= 0),
            UNIQUE (item_name, room_type)
            )
        """)

        # Speed up queries based on commonly used variables
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_item_count_summary_item_room
            ON item_count_summary (item_name, room_type)
        """)

        # -----------------------
        # CARBON STOCK SUMMARY
        # Room level carbon mass
        # -----------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS room_carbon_stock (
            carbon_summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
            room_type TEXT NOT NULL,
            expected_total_carbon_kgC REAL,
            expected_biog_carbon_kgC REAL NOT NULL,
            expected_fossil_carbon_kgC REAL NOT NULL,
            carbon_notes TEXT,
            FOREIGN KEY (room_type) REFERENCES room(room_type),
            CHECK (expected_total_carbon_kgC IS NULL OR expected_total_carbon_kgC >= 0.0),
            CHECK (expected_biog_carbon_kgC >= 0.0),
            CHECK (expected_fossil_carbon_kgC >= 0.0),
            UNIQUE (room_type)
            )
        """)

        # Speed up queries based on commonly used variables
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_room_carbon_stock_room
            ON room_carbon_stock (room_type)
        """)

        # Write changes and print confirmation in terminal
        con.commit()
        print(f"Initialised blank database at: {db_path}")

    # Ensures connection is always closed cleanly, regardless of any other actions
    finally:
        con.close()

# Load helper
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="init_db",
        description="Initialise a blank SQLite database for the Fire Emissions inventory project.",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Database handle from config/local_paths.yaml (e.g. inventory_db, test_db, fire_db).",
    )
    parser.add_argument(
        "--profile",
        required=True,
        help="Profile name from config/local_paths.yaml (e.g. tom, tom_test).",
    )
    args = parser.parse_args(argv)

    config = load_local_paths_config(Path("config") / "local_paths.yaml")
    db_path = resolve_db_path(args.profile, args.db, config)

    init_database(str(db_path))
    return 0

# Ensures that function will not autorun if imported by another script
if __name__ == "__main__":
    raise SystemExit(main())