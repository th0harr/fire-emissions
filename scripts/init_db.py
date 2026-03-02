"""
Initialise a blank SQLite database for the Fire Emissions inventory project.
Note: this only provides the database structue, it will not add items (ingest data)

Run from the project root:
    python scripts/init_db.py
"""

import sqlite3
from pathlib import Path


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
            source_id TEXT NOT NULL,
            room_type TEXT,
            item_description TEXT NOT NULL,
            item_name TEXT,
            count REAL,
            furniture_class TEXT,
            notes TEXT,
            FOREIGN KEY (source_id) REFERENCES sources(source_id) ON DELETE CASCADE
        );
        """)

        # Speed up queries based on commonly used variables
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_inv_source
        ON inventory_observations (source_id);
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_inv_room
        ON inventory_observations (room_type);
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_inv_item_name
        ON inventory_observations (item_name);
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_inv_furniture_class
        ON inventory_observations (furniture_class);
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
            item_description TEXT NOT NULL,
            item_mass REAL,
            furniture_class TEXT,
            notes TEXT
        );
        """)

        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_item_dict_furniture_class
        ON item_dictionary (furniture_class);
        """)

        # -------------------------------------------------------------------------
        # FURNITURE CLASS (curated vocab)
        # Stores category level data
        # From mapping_list.xlsx sheet: "furniture_class"
        # -------------------------------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS furniture_class (
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
        # ROOM TYPE (curated vocab)
        # From mapping_list.xlsx sheet: "room_type"
        # -------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS room_type (
            room_type TEXT PRIMARY KEY,
            notes TEXT
        );
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
            rows_deleted INTEGER        
        );
        """)

        # Write changes and print confirmation in terminal
        con.commit()
        print(f"Initialised blank database at: {db_path}")

    # Ensures connection is always closed cleanly, regardless of any other actions
    finally:
        con.close()

# Ensures that function will not autorun if imported by another script 
if __name__ == "__main__":
    init_database("data/processed/pooled_inventory.sqlite")
