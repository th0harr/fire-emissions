"""
ingest_utils.py

Miscelaneous helper utilities for common data ingestion tasks

Provides:
- content-based hashing of raw source files for unique primary key generation
- database connection helpers
- retrieval of existing source_ids by data_source_type
- schema-tolerant logging of ingests (to ingest_log)
- deletion of all database records associated with a source_id

This module is not tied to any specific source_type as is used by all ingest_<type>.py modules.
"""

from __future__ import annotations

import hashlib   # provides SHA-265 hash used to create source_id
import sqlite3 
from dataclasses import dataclass  # create simple data containers
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable  # typing helpers

# --------------------------------------------------
# Create UTC time 
def utc_now_iso() -> str:
    """Returns current date/time as ISO-8601 string in UTC."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

# --------------------------------------------------
# DB connection helper
def db_connect(db_path: Path) -> sqlite3.Connection:
    """
    Open a SQLite connection to a chosen database
    """
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA foreign_keys = ON;")  # Foreign keys are disabled by default in SQLite
    return con

# --------------------------------------------------
# Create a unique hash (source_id) for primary key
def compute_source_id(file_path: Path, chunk_size: int = 1024 * 1024) -> str:
    """
    Compute SHA-256 hash (64 char hex string) of file contents.
    (Chunks first to avoid loading the entire file into memory.)

    This is used as a stable, content-addressed source_id:
    Stored as sources.source_id
    Note: identical file contents -> identical source_id
    (so any file change -> new source_id)
    """
    file_path = Path(file_path)
    h = hashlib.sha256()
    with file_path.open("rb") as f:  # rb = binary read mode
        while True:
            chunk = f.read(chunk_size)   # reads up to 1MB
            if not chunk:
                break
            h.update(chunk)   # feeds the chunk into the hash
    return h.hexdigest()

# --------------------------------------------------
# Fetch existing source_ids for a data_source_type
def fetch_existing_source_ids(
    con: sqlite3.Connection, 
    data_source_type: str,
) -> set[str]:
    """
    Return all existing source_id values in sources for the given data_source_type.
    """
    rows = con.execute(
        "select source_id from sources where data_source_type = ?",
        (data_source_type,),  # data_source_type is a 1-tuple so trailing comma matters
    ).fetchall() # returns a list
    return {r[0] for r in rows}  # builds a set containing the first column (source_id) of each row

# --------------------------------------------------
# Table inspection for robust logging
# (ingest_log may change so detects what actually exists, rather than what it expects)
# Note: internal function (hence leading underscore)
def _table_columns(con: sqlite3.Connection, table_name: str) -> set[str]:
    """Returns a set of column names for ingest_log (or empty set if table is missing)."""
    rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
    # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, PK
    return {r[1] for r in rows} if rows else set()   # r[1] is column name

# Row insertion
# Note: internal function (hence leading underscore)
def _insert_row(con: sqlite3.Connection, table: str, values: dict[str, Any]) -> None:
    """
    Insert a row using only keys present in the table.
        - keys (k) must be actual columns
        - values (v) must not be None (avoid inserting nulls)
    Ignores extra keys (helps keep code resilient as schema evolves).
    Calls _table_columns() for detection of column names.
    """
    cols = _table_columns(con, table)
    if not cols:
        raise RuntimeError(f"Table '{table}' not found in database.")

    filtered = {k: v for k, v in values.items() if k in cols and v is not None}
    if not filtered:
        # Nothing to insert (e.g. table exists but our provided fields don't match)
        raise RuntimeError(
            f"Table '{table}' exists but none of the provided fields match its columns.\n"
            f"Provided keys: {sorted(values.keys())}\n"
            f"Table columns: {sorted(cols)}"
        )

    # Builds the insert statement dynamically
    keys = list(filtered.keys()) # list of columns to insert
    placeholders = ", ".join(["?"] * len(keys)) # creates a string with right number of placeholders
    sql = f"INSERT INTO {table} ({', '.join(keys)}) VALUES ({placeholders})"
    
    # Executes with the values in matching order
    # (converts to a tuple becayse SQLite's API expects a sequence)
    con.execute(sql, tuple(filtered[k] for k in keys))

# --------------------------------------------------
# Record the ingest run in ingest_log
@dataclass(frozen=True)
class IngestLogEntry:   # Defines a dataclass to store log fields
    """
    A flexible log entry.
    Only fields that exist in your ingest_log table will be inserted.
    (Each is optional; if missing defaults to None.)

    Dataclass can be extended later without breaking older DBs.
    """
    source_id: str | None = None
    data_source_type: str | None = None
    action: str | None = None          # e.g. "ingest", "prune", "delete"
    status: str | None = None          # e.g. "success", "failed", "dry_run"
    message: str | None = None
    file_path: str | None = None
    file_name: str | None = None
    started_utc: str | None = None
    finished_utc: str | None = None
    rows_inserted: int | None = None
    rows_deleted: int | None = None

# Builds a dictionary of candidate -> value pairs
def record_ingest_run(con: sqlite3.Connection, entry: IngestLogEntry) -> None:
    """
    Insert a row into ingest_log.

    Should be schema-tolerant:
    it will insert only the fields that exist in the current ingest_log table.

    Calls _insert_row() to identify/insert real columns and keys
    """
    values = {
        "source_id": entry.source_id,
        "data_source_type": entry.data_source_type,
        "action": entry.action,
        "status": entry.status,
        "message": entry.message,
        "file_path": entry.file_path,
        "file_name": entry.file_name,
        "started_utc": entry.started_utc,
        "finished_utc": entry.finished_utc,
        "rows_inserted": entry.rows_inserted,
        "rows_deleted": entry.rows_deleted,
        # Possible columns that might used in future schema (add/delete as necessary):
        "date_started_utc": entry.started_utc,
        "date_finished_utc": entry.finished_utc,
        "notes": entry.message,
    }
    _insert_row(con, "ingest_log", values)


# --------------------------------------------------
# Delete all items associated with a source_id
# To allow easy removal of all items associated with an invalid ingest or raw data file
@dataclass(frozen=True)
class DeleteSummary:
    """Produces structured return value of deleted items"""
    source_id: str
    observations_deleted: int
    sources_deleted: int

def delete_by_source_id(con: sqlite3.Connection, source_id: str) -> DeleteSummary:
    """
    Deletes all inventory_observations linked to source_id, then deletes the sources row.

    Intended for:
      - --prune candidates (missing raw files)
      - explicit delete operations ()

    Does NOT delete ingest_log rows (audit trail is useful).
    """
    cur = con.cursor()

    cur.execute("DELETE FROM inventory_observations WHERE source_id = ?", (source_id,))
    obs_deleted = cur.rowcount if cur.rowcount is not None else 0

    cur.execute("DELETE FROM sources WHERE source_id = ?", (source_id,))
    src_deleted = cur.rowcount if cur.rowcount is not None else 0

    # Returns a structured object summarisong the outcome
    return DeleteSummary(
        source_id=source_id,
        observations_deleted=obs_deleted,
        sources_deleted=src_deleted,
    )
