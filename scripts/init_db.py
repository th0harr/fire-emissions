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
            item_raw TEXT NOT NULL,
            item_name TEXT,
            count REAL,
            furniture_type TEXT,
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
        CREATE INDEX IF NOT EXISTS idx_inv_furniture_type
        ON inventory_observations (furniture_type);
        """)

        # -------------------------------------
        # ITEM DICTIONARY
        # Controlled vocabulary / mapping table
        # Creates the canonical list of items
        # -------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS item_dictionary (
            item_name TEXT PRIMARY KEY,
            example_item_raw TEXT,
            furniture_type TEXT,
            first_seen_utc TEXT,
            last_seen_utc TEXT,
            notes TEXT
        );
        """)

        # -------------------------------------------------------------------------
        # FURNITURE TYPES
        # Stores category level emissions data
        # Can be expanded later if required (e.g. adding fossil/biogenic fraction)
        # -------------------------------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS furniture_types (
            furniture_type TEXT PRIMARY KEY,
            description TEXT,
            kgco2e_per_kg REAL,
            source_ref TEXT,
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
