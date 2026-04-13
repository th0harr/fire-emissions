from __future__ import annotations

import re   # Parsing and normalization
import sqlite3   # SQLite handling
import uuid   # Required for source_id creation
from dataclasses import dataclass, field   # Neat storage of intermediate data
from pathlib import Path
from typing import Any

import pandas as pd   # Excel reading

from scripts.ingest_utils import IngestLogEntry, db_connect, record_ingest_run, utc_now_iso


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Contains all DB schema and survey mappings in one place
# for easy modification

# Raw input file must be named: "jisc_survey_export.xlsx"
EXPECTED_FILENAME = "jisc_survey_export.xlsx"

# Source metadata
SOURCE_TYPE = "survey"
SOURCE_DESCRIPTION = "JISC survey export"
SOURCE_ORG = "JISC"

# Section mappings (i.e. the survey blueprint)
# If survey is modified this may need to be changed.
SECTION_CONFIG: dict[int, dict[str, Any]] = {
    1: {"section_role": "inventory", "room_type": "living_room", "comment_type": "comment_living_room"},
    2: {"section_role": "inventory", "room_type": "living_room", "comment_type": "comment_living_room"},
    3: {"section_role": "inventory", "room_type": "bedroom", "comment_type": "comment_bedroom"},
    4: {"section_role": "inventory", "room_type": "bedroom", "comment_type": "comment_bedroom"},
    5: {"section_role": "inventory", "room_type": "kitchen", "comment_type": "comment_kitchen"},
    6: {"section_role": "inventory", "room_type": "kitchen", "comment_type": "comment_kitchen"},
    7: {"section_role": "dwelling", "comment_type": "comment_room_type"},
    8: {"section_role": "inventory", "room_type": "unspecified_room"}, # assigns to a generic bucket-room
    9: {"section_role": "dwelling", "comment_type": "comment_room_type"},
    10:{"section_role": "comment_only", "comment_type": "comment_general"},
}

# Table names for inventory_db. Adjust here if schema is changed.
TABLE_SOURCES = "sources"
TABLE_ITEMS = "item_dictionary"
TABLE_ROOMS = "room"
TABLE_INVENTORY = "inventory_observations"
TABLE_DWELLING = "dwelling_observations"
TABLE_COMMENTS = "survey_comments"

# Header regexes (section.number.* text)
COUNT_HEADER_RE = re.compile(r"^\s*(\d+)\.(\d+)\.\s*(.*?)\s*$")
SECTION_HEADER_RE = re.compile(r"^\s*(\d+)\.\s*(.*?)\s*$")


# -----------------------------------------------------------------------------
# Data structures
# -----------------------------------------------------------------------------

# Deconstructs the header to find relevant content
# Note: headers are the survey questions/statements.
# Each header starts with a section_number ...
# and may have a question number (if a count)
# it may also have a "*".
# Finally, it will have a text string.
# Basically: 'what does this header look like?'
@dataclass
class ParsedHeader:
    column_index: int
    raw_header: str
    header_type: str  # response_id | metadata | count | section
    section_number: int | None = None
    question_number: int | None = None
    description_text: str | None = None

# Second-pass version of parsed header
# Appends associated mappings with DB
# (based on salient contents of text string)
# Basically: 'what should I do with this header?'
@dataclass
class ResolvedHeader:
    column_index: int
    raw_header: str
    header_type: str
    section_number: int | None = None
    question_number: int | None = None
    description_text: str | None = None
    destination: str | None = None
    item_name: str | None = None
    room_type: str | None = None
    comment_type: str | None = None

# Stores the the entire dry-run result for one input file
# (from --scan)
@dataclass
class FilePlan:
    file_path: str
    file_name: str
    n_rows: int = 0
    n_headers: int = 0
    # Creates an empty list for each new instance...
    parsed_headers: list[ParsedHeader] = field(default_factory=list)
    resolved_headers: list[ResolvedHeader] = field(default_factory=list)
    inventory_rows: list[dict[str, Any]] = field(default_factory=list)
    dwelling_rows: list[dict[str, Any]] = field(default_factory=list)
    comment_rows: list[dict[str, Any]] = field(default_factory=list)
    response_ids_in_file: list[str] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    warnings: list[dict[str, Any]] = field(default_factory=list)

    # Decide wether FilePlan has any blocking errors
    @property
    def has_blocking_errors(self) -> bool:
        return has_blocking_errors(self.errors)


# -----------------------------------------------------------------------------
# Public ingest interface
# -----------------------------------------------------------------------------

# Top-level functions to fit to ingest architecture
# i.e. what ingest.py would call (via CLIs)

# Scan raw survey file
def scan_inputs(raw_dir: str | Path) -> list[Path]:
    """Returns the single expected survey export file from the raw directory
    when running ``--scan``.

    This ingest is intentionally restricted to one canonical filename,
    ``jisc_survey_export.xlsx``.
    The file is treated as the latest snapshot of survey data
    and is MANUALLY overwritten by newer exports.
    """
    target = Path(raw_dir) / EXPECTED_FILENAME
    return [target] if target.exists() else []

# Plan ingest
def plan(db_path: str | Path, raw_dir: str | Path, input_files: list[str | Path]) -> dict[str, Any]:
    """Perform a dry-run parse and validation for one or more survey files.

    This function loads the canonical item and room vocabularies from the DB,
    plans each input file, and returns a dict matching the interface expected by
    ingest.py.
    """
    # From ingest_utils.py
    conn = db_connect(Path(db_path))
    conn.row_factory = sqlite3.Row

    try:
        item_lookup = load_item_lookup(conn)
        room_lookup = load_room_lookup(conn)

        summaries: list[dict[str, Any]] = []
        new_files: list[str] = []
        existing_files: list[str] = []

        for input_file in input_files:
            file_plan = plan_one_file(conn, input_file, item_lookup, room_lookup)
            summary = summarise_plan_result(file_plan)
            summaries.append(summary)

            # For survey ingest, the canonical file is always treated as the
            # current candidate input. The overwrite / deduplication logic
            # happens inside ingest_apply().
            new_files.append(input_file)

        return {
            "new": new_files,
            "existing": existing_files,
            "details": summaries,
        }
    finally:
        conn.close()

# Preview prune candidates
def prune_preview(db_path: str | Path, raw_dir: str | Path) -> dict[str, Any]:
    """Preview obsolete survey responses that would be removed by ``--prune``.

    The latest JISC export is treated as authoritative for the active set of
    response IDs. Any survey-derived DB rows whose response_id is absent from
    the current file are reported as prune candidates.
    """
    target = Path(raw_dir) / EXPECTED_FILENAME
    if not target.exists():
        return {
            "type": SOURCE_TYPE,
            "file_found": False,
            "file_path": str(target),
            "obsolete_response_ids": [],
            "obsolete_count": 0,
            "row_counts": {TABLE_INVENTORY: 0, TABLE_DWELLING: 0, TABLE_COMMENTS: 0},
        }

    # Try running the code and catch any errors instead of crashing
    try:
        # Create data frame
        df = pd.read_excel(target, dtype=object)
    # Store any exception object (error) in exc
    except Exception as exc:
        return {
            "type": SOURCE_TYPE,
            "file_found": True,
            "file_path": str(target),
            "obsolete_response_ids": [],
            "obsolete_count": 0,
            "row_counts": {TABLE_INVENTORY: 0, TABLE_DWELLING: 0, TABLE_COMMENTS: 0},
            "error": {"type": "read_excel_failed", "detail": str(exc)},
        }

    # Extract list of response_id from file
    if df.empty:
        latest_ids: set[str] = set()
    else:
        latest_ids = extract_response_ids_from_dataframe(df)

    # From ingest_utils.py
    conn = db_connect(Path(db_path))
    conn.row_factory = sqlite3.Row

    # Find obsolete response_ids (in DB but not survey export)
    try:
        existing_ids = get_existing_survey_response_ids(conn)
        obsolete_ids = sorted(existing_ids - latest_ids)
        row_counts = count_rows_for_response_ids(conn, obsolete_ids)
        return {
            "type": SOURCE_TYPE,
            "file_found": True,
            "file_path": str(target),
            "obsolete_response_ids": obsolete_ids,
            "obsolete_count": len(obsolete_ids),
            "row_counts": row_counts,
        }
    finally:
        conn.close()

# Apply pruning to prune candidates
def prune_apply(db_path: str | Path, raw_dir: str | Path) -> dict[str, Any]:
    """Delete obsolete survey rows identified by :func:`prune_preview`,
    using ``--prune --apply``.

    Rows are removed from the survey-derived observation and comment tables for
    response IDs no longer present in the latest survey export.
    Orphan survey source rows are cleaned up afterward.
    """
    preview = prune_preview(db_path, raw_dir)
    obsolete_ids = preview["obsolete_response_ids"]

    if not obsolete_ids:
        preview["applied"] = True
        preview["deleted"] = {TABLE_INVENTORY: 0, TABLE_DWELLING: 0, TABLE_COMMENTS: 0, TABLE_SOURCES: 0}
        return preview

    # From ingest_utils.py
    conn = db_connect(Path(db_path))
    conn.row_factory = sqlite3.Row
    started = utc_now_iso()

    try:
        conn.execute("BEGIN")
        deleted_counts = delete_survey_rows_for_response_ids(conn, obsolete_ids)
        deleted_counts[TABLE_SOURCES] = cleanup_orphan_survey_sources(conn)
        
        conn.commit()

        # Ingest logging
        try:
            record_ingest_run(
                conn,
                IngestLogEntry(
                    data_source_type=SOURCE_TYPE,
                    action="prune",
                    status="success",
                    message=f"Pruned {len(obsolete_ids)} obsolete survey response_id values.",
                    file_path=preview.get("file_path"),
                    file_name=Path(preview.get("file_path", EXPECTED_FILENAME)).name,
                    started_utc=started,
                    finished_utc=utc_now_iso(),
                    rows_deleted=sum(deleted_counts.values()),
                ),
            )
            conn.commit()
        except Exception:
            pass

        preview["applied"] = True
        preview["deleted"] = deleted_counts
        return preview

    # Ingest logging
    except Exception as exc:
        conn.rollback()
        try:
            record_ingest_run(
                conn,
                IngestLogEntry(
                    data_source_type=SOURCE_TYPE,
                    action="prune",
                    status="failed",
                    message=str(exc),
                    file_path=preview.get("file_path"),
                    file_name=Path(preview.get("file_path", EXPECTED_FILENAME)).name,
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
    

# Apply ingestion
def ingest_apply(db_path: str | Path, raw_dir: str | Path, new_files: list[str]) -> list[dict[str, Any]]:
    """Validate and ingest the survey export into the database.
    Requires ``--scan --apply``. Note: destructive operation.

    For each file, this function first builds a full plan. If validation passes,
    any existing survey rows for the same response IDs are deleted and the file
    is reinserted inside a transaction, making repeated imports idempotent.
    (i.e. does not duplicate existing rows).
    """
    # From ingest_utils.py
    conn = db_connect(Path(db_path))
    conn.row_factory = sqlite3.Row

    try:
        item_lookup = load_item_lookup(conn)
        room_lookup = load_room_lookup(conn)

        results: list[dict[str, Any]] = []
        # Re-runs planning step
        for input_file in new_files:
            file_plan = plan_one_file(conn, input_file, item_lookup, room_lookup)

            if file_plan.has_blocking_errors:
                try:
                    record_ingest_run(
                        conn,
                        IngestLogEntry(
                            data_source_type=SOURCE_TYPE,
                            action="ingest",
                            status="failed",
                            message="blocking validation errors",
                            file_path=file_plan.file_path,
                            file_name=file_plan.file_name,
                            started_utc=utc_now_iso(),
                            finished_utc=utc_now_iso(),
                        ),
                    )
                except Exception:
                    pass

                results.append({
                    "file": file_plan.file_name,
                    "applied": False,
                    "reason": "blocking validation errors",
                    "summary": summarise_plan_result(file_plan),
                })
                continue

            started = utc_now_iso()

            try:
                conn.execute("BEGIN")

                # Remove any existing survey rows for response_ids present in this file.
                # This makes repeated imports idempotent even if --prune is not run.
                delete_survey_rows_for_response_ids(conn, file_plan.response_ids_in_file)
                cleanup_orphan_survey_sources(conn)

                source_id = insert_source_row(conn, input_file)
                insert_inventory_rows(conn, source_id, file_plan.inventory_rows)
                insert_dwelling_rows(conn, source_id, file_plan.dwelling_rows)
                insert_comment_rows(conn, source_id, file_plan.comment_rows)

                conn.commit()

                # Ingest logging
                try:
                    record_ingest_run(
                        conn,
                        IngestLogEntry(
                            source_id=source_id,
                            data_source_type=SOURCE_TYPE,
                            action="ingest",
                            status="success",
                            message=f"Imported survey export with {len(file_plan.response_ids_in_file)} response_id values.",
                            file_path=file_plan.file_path,
                            file_name=file_plan.file_name,
                            started_utc=started,
                            finished_utc=utc_now_iso(),
                            rows_inserted=(
                                len(file_plan.inventory_rows)
                                + len(file_plan.dwelling_rows)
                                + len(file_plan.comment_rows)
                            ),
                        ),
                    )
                    conn.commit()
                except Exception:
                    pass

                results.append({
                    "file": file_plan.file_name,
                    "applied": True,
                    "source_id": source_id,
                    "inventory_rows": len(file_plan.inventory_rows),
                    "dwelling_rows": len(file_plan.dwelling_rows),
                    "comment_rows": len(file_plan.comment_rows),
                    "warnings": file_plan.warnings,
                })

            except Exception as exc:
                conn.rollback()
                try:
                    record_ingest_run(
                        conn,
                        IngestLogEntry(
                            data_source_type=SOURCE_TYPE,
                            action="ingest",
                            status="failed",
                            message=str(exc),
                            file_path=file_plan.file_path,
                            file_name=file_plan.file_name,
                            started_utc=started,
                            finished_utc=utc_now_iso(),
                        ),
                    )
                    conn.commit()
                except Exception:
                    pass

                results.append({
                    "file": file_plan.file_name,
                    "applied": False,
                    "reason": str(exc),
                    "summary": summarise_plan_result(file_plan),
                })

        return results

    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Core planning logic
# -----------------------------------------------------------------------------

# Main survey file interpretation logic
def plan_one_file(
    conn: sqlite3.Connection,
    input_file: str | Path,
    item_lookup: dict[str, dict[str, str]],
    room_lookup: dict[str, dict[str, str]],
) -> FilePlan:
    """Read, parse, validate, and stage rows for a single survey workbook.

    The returned :class:`FilePlan` contains parsed headers, candidate DB rows,
    discovered response IDs, and any structured validation errors.

    Called by :func:`plan`, :func:`ingest_apply`.
    """
    # Filepaths
    path = Path(input_file)
    out = FilePlan(file_path=str(path), file_name=path.name)

    # Check basic validity
    if not path.exists():
        out.errors.append(error_record("missing_file", file=str(path)))
        return out

    try:
        df = pd.read_excel(path, dtype=object)
    except Exception as exc:
        out.errors.append(error_record("read_excel_failed", file=str(path), detail=str(exc)))
        return out

    out.n_rows, out.n_headers = df.shape

    if out.n_headers == 0:
        out.errors.append(error_record("invalid_file_structure", file=str(path), detail="no columns found"))
        return out

    if out.n_rows == 0:
        out.errors.append(error_record("empty_workbook", file=str(path)))
        return out

    # Parse all headers
    out.parsed_headers = parse_headers(df.columns, out.errors)
    # Resolve headers against DB vocab
    out.resolved_headers = resolve_headers_to_targets(out.parsed_headers, item_lookup, room_lookup, out.errors)

    if has_blocking_errors(out.errors):
        return out

    response_ids_seen: set[str] = set()

    # Process every row/response
    for row_index, row in enumerate(df.itertuples(index=False, name=None), start=1):
        inventory_rows, dwelling_rows, comment_rows, response_id = process_response_row(
            row=row,
            row_index=row_index,
            header_map=out.resolved_headers,
            errors=out.errors,
            warnings=out.warnings,
        )
        
        # Build candidate insert rows
        out.inventory_rows.extend(inventory_rows)
        out.dwelling_rows.extend(dwelling_rows)
        out.comment_rows.extend(comment_rows)

        # Check for duplicate response_ids (return structured error)
        if response_id is not None:
            if response_id in response_ids_seen:
                out.errors.append(error_record(
                    "duplicate_response_id_in_file",
                    response_id=response_id,
                    row_index=row_index,
                ))
            response_ids_seen.add(response_id)

    out.response_ids_in_file = sorted(response_ids_seen)
    return out


# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------

# Find mapped item_name from item_description
# (Uses item_dictionary table created from mapping_list.xlsx)
def load_item_lookup(conn: sqlite3.Connection) -> dict[str, dict[str, str]]:
    """Load canonical item descriptions from the DB for exact header matching.

    Keys are stored as normalised ``item_description`` strings and values hold
    the canonical ``item_name`` plus original description text.

    Note, this assumes that item_descriptions match survey header text.
    
    Called by :func:`ingest_apply`.
    """
    sql = f"SELECT item_name, item_description FROM {TABLE_ITEMS}"
    rows = conn.execute(sql).fetchall()

    lookup: dict[str, dict[str, str]] = {}
    for r in rows:
        desc = normalise_text(r["item_description"])
        if desc in lookup:
            raise ValueError(f"Duplicate normalized item_description in DB: {r['item_description']!r}")
        lookup[desc] = {
            "item_name": r["item_name"],
            "item_description": r["item_description"],
        }
    return lookup

# Find matched room_type from room_description
# (Uses room table created from mapping_list.xlsx)
def load_room_lookup(conn: sqlite3.Connection) -> dict[str, dict[str, str]]:
    """Load canonical room descriptions from the DB for dwelling matching.

    Keys are normalised ``room_description`` strings and values hold the
    canonical ``room_type`` plus original description text.
    
    Note, this assumes that room_descriptions match survey header text.

    Called by :func:`ingest_apply`.
    """
    sql = f"SELECT room_type, room_description FROM {TABLE_ROOMS}"
    rows = conn.execute(sql).fetchall()

    lookup: dict[str, dict[str, str]] = {}
    for r in rows:
        desc = normalise_text(r["room_description"])
        if desc in lookup:
            raise ValueError(f"Duplicate normalized room_description in DB: {r['room_description']!r}")
        lookup[desc] = {
            "room_type": r["room_type"],
            "room_description": r["room_description"],
        }
    return lookup

# Get list of all existing response_ids in DB
def get_existing_survey_response_ids(conn: sqlite3.Connection) -> set[str]:
    """Return all distinct response IDs currently stored for survey data only.

    The query is restricted to rows linked to ``sources.data_source_type =
    'survey'`` so prune logic cannot accidentally act on non-survey data.
    
    Called by :func:`prune_preview`.
    """
    # Checks the following 3 tables for existing response_ids
    # as not all objects go into each table
    sql = f"""
        SELECT DISTINCT response_id
        FROM (
            SELECT io.response_id AS response_id
            FROM {TABLE_INVENTORY} io
            JOIN {TABLE_SOURCES} s ON s.source_id = io.source_id
            WHERE s.data_source_type = ?

            UNION

            SELECT do.response_id AS response_id
            FROM {TABLE_DWELLING} do
            JOIN {TABLE_SOURCES} s ON s.source_id = do.source_id
            WHERE s.data_source_type = ?

            UNION

            SELECT sc.response_id AS response_id
            FROM {TABLE_COMMENTS} sc
            JOIN {TABLE_SOURCES} s ON s.source_id = sc.source_id
            WHERE s.data_source_type = ?
        )
        WHERE response_id IS NOT NULL
    """
    rows = conn.execute(sql, (SOURCE_TYPE, SOURCE_TYPE, SOURCE_TYPE)).fetchall()
    return {str(r["response_id"]) for r in rows if r["response_id"] is not None}

# Counts number of rows that would be pruned.
def count_rows_for_response_ids(conn: sqlite3.Connection, response_ids: list[str]) -> dict[str, int]:
    """Count survey-derived rows linked to a list of response IDs.

    Used by prune preview to show how many rows in each destination table would
    be affected by deletion.
    
    Called by :func:`prune_preview`.
    """
    if not response_ids:
        return {TABLE_INVENTORY: 0, TABLE_DWELLING: 0, TABLE_COMMENTS: 0}

    # Builds the right number of SQL parameter placeholders
    # for a variable_length IN (...) query.
    # (replaces every list-item with "?").
    placeholders = ",".join("?" for _ in response_ids)
    params = [SOURCE_TYPE, *response_ids]

    counts = {}
    for table, alias in ((TABLE_INVENTORY, "io"), (TABLE_DWELLING, "do"), (TABLE_COMMENTS, "sc")):
        sql = f"""
            SELECT COUNT(*) AS n
            FROM {table} {alias}
            JOIN {TABLE_SOURCES} s ON s.source_id = {alias}.source_id
            WHERE s.data_source_type = ?
              AND {alias}.response_id IN ({placeholders})
        """
        counts[table] = int(conn.execute(sql, params).fetchone()["n"])
    return counts

# Deletes survey rows linked to speficifed respponse_ids
def delete_survey_rows_for_response_ids(conn: sqlite3.Connection, response_ids: list[str]) -> dict[str, int]:
    """Delete survey rows for the supplied response IDs.

    Deletion is restricted to rows whose source belongs to the survey ingest.
    The returned dictionary reports how many rows were removed per table.
    
    Called by :func:`prune_apply`.
    """
    if not response_ids:
        return {TABLE_INVENTORY: 0, TABLE_DWELLING: 0, TABLE_COMMENTS: 0}

    placeholders = ",".join("?" for _ in response_ids)
    deleted: dict[str, int] = {}

    for table, alias in ((TABLE_INVENTORY, "io"), (TABLE_DWELLING, "do"), (TABLE_COMMENTS, "sc")):
        sql = f"""
            DELETE FROM {table}
            WHERE rowid IN (
                SELECT {alias}.rowid
                FROM {table} {alias}
                JOIN {TABLE_SOURCES} s ON s.source_id = {alias}.source_id
                WHERE s.data_source_type = ?
                  AND {alias}.response_id IN ({placeholders})
            )
        """
        cur = conn.execute(sql, (SOURCE_TYPE, *response_ids))
        deleted[table] = cur.rowcount if cur.rowcount != -1 else 0

    return deleted

# Tidy source rows linked to deleted response_ids
def cleanup_orphan_survey_sources(conn: sqlite3.Connection) -> int:
    """Remove survey source rows that no longer own any child records.

    This keeps the ``sources`` table tidy after response-level replacement or
    prune operations.
    
    Called by :func:`prune_apply`, :func:`ingest_apply`.
    """
    sql = f"""
        DELETE FROM {TABLE_SOURCES}
        WHERE data_source_type = ?
          AND source_id NOT IN (
              SELECT source_id FROM {TABLE_INVENTORY}
              UNION
              SELECT source_id FROM {TABLE_DWELLING}
              UNION
              SELECT source_id FROM {TABLE_COMMENTS}
          )
    """
    cur = conn.execute(sql, (SOURCE_TYPE,))
    return cur.rowcount if cur.rowcount != -1 else 0


# -----------------------------------------------------------------------------
# Parsing and mapping helpers
# -----------------------------------------------------------------------------

# Functions for understanding the survey structure

# Normalises text for more reliable case matching
def normalise_text(value: Any) -> str:
    """Normalise descriptive text for strict controlled-vocabulary matching.

    This lowers case, trims whitespace, removes trailing asterisks, and
    collapses repeated spaces without otherwise changing the phrase.
    """
    if value is None:
        return ""
    text = str(value).strip()
    text = re.sub(r"\*+$", "", text).strip()
    text = text.lower()
    text = re.sub(r"\s+", " ", text)
    return text

# Decides on col data type based on header structure (or location)
def parse_headers(columns: Any, errors: list[dict[str, Any]]) -> list[ParsedHeader]:
    """Classify spreadsheet headers into response, count, section, or metadata.

    Count headers follow ``section.question. text`` and section headers follow
    ``section. text``. Any non-matching columns are treated as metadata.
    """
    parsed: list[ParsedHeader] = []

    for idx, raw in enumerate(columns):
        raw_header = "" if raw is None else str(raw)

        # Col 1 is always response_id
        if idx == 0:
            parsed.append(ParsedHeader(
                column_index=idx,
                raw_header=raw_header,
                header_type="response_id",
            ))
            continue
        
        # Headers containing section_number.question_number are always count columns
        m_count = COUNT_HEADER_RE.match(raw_header)
        if m_count:
            parsed.append(ParsedHeader(
                column_index=idx,
                raw_header=raw_header,
                header_type="count",
                section_number=int(m_count.group(1)),
                question_number=int(m_count.group(2)),
                description_text=clean_header_description(m_count.group(3)),
            ))
            continue

        # Headers with section_number but NOT question_number,
        # can be either section header OR comment column.
        m_section = SECTION_HEADER_RE.match(raw_header)
        if m_section:
            parsed.append(ParsedHeader(
                column_index=idx,
                raw_header=raw_header,
                header_type="section",
                section_number=int(m_section.group(1)),
                description_text=clean_header_description(m_section.group(2)),
            ))
            continue

        # Headers with no section_number (and not col1) are metadata
        parsed.append(ParsedHeader(
            column_index=idx,
            raw_header=raw_header,
            header_type="metadata",
        ))

    return parsed

# Cleans the header for better case matching
def clean_header_description(text: str | None) -> str:
    """Strip and lightly clean the descriptive text extracted from a header."""
    return re.sub(r"\s+", " ", (text or "").strip()).rstrip("*").strip()

# 
def resolve_headers_to_targets(
    parsed_headers: list[ParsedHeader],
    item_lookup: dict[str, dict[str, str]],
    room_lookup: dict[str, dict[str, str]],
    errors: list[dict[str, Any]],
) -> list[ResolvedHeader]:
    """Resolve parsed headers to concrete DB targets and canonical entities.

    Inventory count headers must map to canonical item descriptions, dwelling
    count headers must map to canonical room descriptions, and section headers
    are mapped to comment types using the fixed section configuration.
    """
    resolved_headers: list[ResolvedHeader] = []

    for h in parsed_headers:
        resolved = ResolvedHeader(
            column_index=h.column_index,
            raw_header=h.raw_header,
            header_type=h.header_type,
            section_number=h.section_number,
            question_number=h.question_number,
            description_text=h.description_text,
        )

        if h.header_type in {"response_id", "metadata"}:
            resolved_headers.append(resolved)
            continue
        
        # Section mappings should exist in Config (start of script)
        if h.section_number not in SECTION_CONFIG:
            errors.append(error_record(
                "unknown_section_number",
                header=h.raw_header,
                section_number=h.section_number,
            ))
            resolved_headers.append(resolved)
            continue

        cfg = SECTION_CONFIG[h.section_number]

        # Parse count-type header for dictionary matching
        if h.header_type == "count":
            
            # Check for item_description within header text
            if cfg["section_role"] == "inventory":
                norm_desc = normalise_text(h.description_text)
                
                # Structured error is known item_description not found
                if norm_desc not in item_lookup:
                    errors.append(error_record(
                        "unmapped_item_description",
                        header=h.raw_header,
                        section_number=h.section_number,
                        item_description=h.description_text,
                    ))
                    resolved_headers.append(resolved)
                    continue

                resolved.destination = TABLE_INVENTORY
                resolved.item_name = item_lookup[norm_desc]["item_name"]
                resolved.room_type = cfg.get("room_type")
                resolved_headers.append(resolved)
                continue
            
            # Check for item_description within header text
            if cfg["section_role"] == "dwelling":
                norm_desc = normalise_text(h.description_text)
                
                # Structured error is known room_description not found
                if norm_desc not in room_lookup:
                    errors.append(error_record(
                        "unmapped_room_description",
                        header=h.raw_header,
                        section_number=h.section_number,
                        room_description=h.description_text,
                    ))
                    resolved_headers.append(resolved)
                    continue

                resolved.destination = TABLE_DWELLING
                resolved.room_type = room_lookup[norm_desc]["room_type"]
                resolved_headers.append(resolved)
                continue

            # For comment only columns
            if cfg["section_role"] == "comment_only":
                errors.append(error_record(
                    "unexpected_count_field_in_comment_section",
                    header=h.raw_header,
                    section_number=h.section_number,
                ))
                resolved_headers.append(resolved)
                continue
        
        # Separate section titles from comment columns
        if h.header_type == "section":
            comment_type = cfg.get("comment_type")
            if comment_type is None:
                # Section-title columns are structural and comments are optional
                # so these columns can be blank/empty
                resolved_headers.append(resolved)
                continue

            resolved.destination = TABLE_COMMENTS
            resolved.comment_type = comment_type
            resolved_headers.append(resolved)
            continue

    return resolved_headers

# Processes the row data based on the expected data type (from parsed header)
def process_response_row(
    row: tuple[Any, ...],
    row_index: int,
    header_map: list[ResolvedHeader],
    errors: list[dict[str, Any]],
    warnings: list[dict[str, Any]], # non-blocking discrepencies
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], str | None]:    
    """
    Transform one respondent row into candidate inventory, dwelling, and comment rows.

    Count fields are coerced to integers, with blank or ``None`` values treated
    as zero only for count-structured headers.

    Non-comment section columns must be blank.
    Any non-blank values implies a survey->mappings mismatch.
    """
    inventory_rows: list[dict[str, Any]] = []
    dwelling_rows: list[dict[str, Any]] = []
    comment_rows: list[dict[str, Any]] = []

    raw_response_id = row[0] if row else None
    response_id = get_response_id(raw_response_id)
    
    # Should never be the case, but here just in case a missing response_id creates an error
    if response_id is None:
        errors.append(error_record("missing_response_id", row_index=row_index))
        return inventory_rows, dwelling_rows, comment_rows, None

    for h in header_map:
        cell_value = row[h.column_index]

        # No processing required for these columns
        if h.header_type in {"response_id", "metadata"}:
            continue
        
        # For count-type columns...
        if h.header_type == "count":

            # Convert all count values to non-negative integers
            count_value, count_error = coerce_count_value(
                raw_value=cell_value,
                raw_header=h.raw_header,
                response_id=response_id,
                row_index=row_index,
            )
            # Row-level error handling
            if count_error is not None:
                errors.append(count_error)
                continue
            
            # Process item counts into inventory rows
            if h.destination == TABLE_INVENTORY:
                inventory_rows.append({
                    "response_id": response_id,
                    "source_id": None,
                    "room_type": h.room_type,
                    "item_name": h.item_name,
                    "count": count_value,
                })
                continue
            
            # Process dwelling counts into room rows
            if h.destination == TABLE_DWELLING:
                dwelling_rows.append({
                    "response_id": response_id,
                    "source_id": None,
                    "room_type": h.room_type,
                    "count": count_value,
                    "assumption_notes": None,
                })
                continue
        
        # For section-type columns...
        if h.header_type == "section":
            # Ignore blank comment cells (comments are optional)          
            if is_blank(cell_value):
                continue
            
            # Check config mappings (script start) for expected comments
            # AND ensure that section-headers are blank
            if h.destination == TABLE_COMMENTS and h.comment_type is not None:
                comment_rows.append({
                    "response_id": response_id,
                    "source_id": None,
                    "comment_type": h.comment_type,
                    "comment_text": str(cell_value).strip(),
                })
                continue
            
            # Non-blank text in a structural section-title column suggests the
            # survey structure and SECTION_CONFIG are out of sync.
            errors.append(error_record(
                "unexpected_text_in_section_title_column",
                header=h.raw_header,
                section_number=h.section_number,
                response_id=response_id,
                row_index=row_index,
                raw_value=str(cell_value),
                instruction="Update SECTION_CONFIG: this section-title column contains text but has no comment mapping.",
            ))
            continue

    dwelling_rows = reconcile_combo_room_counts(
        response_id=response_id,
        row_index=row_index,
        dwelling_rows=dwelling_rows,
        warnings=warnings,
    )        

    return inventory_rows, dwelling_rows, comment_rows, response_id

# Get the response_id value 
def get_response_id(value: Any) -> str | None:
    """Return the raw JISC response_id if present, otherwise None.

    This identifier is treated as case-sensitive and is not normalised.
    """
    if is_blank(value):
        return None
    return str(value)

# Checks whether a cell is effectively empty
def is_blank(value: Any) -> bool:
    """Treat None, NaN, and empty/whitespace strings as blank."""
    if value is None:
        return True
    if pd.isna(value):
        return True
    return str(value).strip() == ""

# Enforces count rules.
def coerce_count_value(
    raw_value: Any,
    raw_header: str,
    response_id: str,
    row_index: int,
) -> tuple[int | None, dict[str, Any] | None]:
    """Convert a raw survey count cell to a non-negative integer.

    Valid blanks and the literal string ``None`` are interpreted as zero for
    count headers. This is due to JISC/excel handling. 
    Any other non-integer or negative value is reported as a
    structured validation error.

    Note, any other value within these cells suggests a mismatch between
    the survey output and our expectations.
    Likely due to changing the survey without updating the config mappings.
    """
    
    # Counts of zero seem to show as blank (either Jisc or Excel)
    if is_blank(raw_value):
        return 0, None

    # Jisc inserts "None" when a count question is unanswered
    raw_text = str(raw_value).strip()
    if raw_text.lower() == "none":
        return 0, None

    # JISC upper-bin responses currently appears as strings like "10+".
    # For now, coerce these to the lower bound integer (e.g. "10+" -> 10).
    # (Obviosuly this is not a long-term fix and the survey needs changing)
    if raw_text.endswith("+"):
        raw_text = raw_text[:-1].strip()

    # Illegal values
    try:
        if isinstance(raw_value, bool):
            raise ValueError("boolean not allowed")

        if isinstance(raw_value, int):
            count = raw_value
        elif isinstance(raw_value, float):
            if not raw_value.is_integer():
                raise ValueError("non-integer float")
            count = int(raw_value)
        else:
            numeric = float(raw_text)
            if not numeric.is_integer():
                raise ValueError("non-integer numeric string")
            count = int(numeric)

        if count < 0:
            raise ValueError("negative count")

        return count, None
    
    # Structured error output
    except Exception:
        return None, error_record(
            "invalid_count_value",
            header=raw_header,
            response_id=response_id,
            row_index=row_index,
            raw_value=repr(raw_value),
        )

# Gets the response_id from the dataframe (col 1)
def extract_response_ids_from_dataframe(df: pd.DataFrame) -> set[str]:
    """Extract the unique set of non-blank response IDs from column 1."""
    if df.shape[1] == 0:
        return set()
    ids: set[str] = set()
    for value in df.iloc[:, 0].tolist():
        rid = get_response_id(value)
        if rid is not None:
            ids.add(rid)
    return ids

# Automatic assumption transparency
def append_assumption_note(row: dict[str, Any], note: str) -> None:
    """Append an assumption note to a staged dwelling row."""
    existing = row.get("assumption_notes")
    if existing: # append to existing note string
        row["assumption_notes"] = f"{existing}; {note}"
    else: # create new note
        row["assumption_notes"] = note

# Automatic assumption: combo + separate rooms
def reconcile_combo_room_counts(
    response_id: str,
    row_index: int,
    dwelling_rows: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Reconcile suspicious combinations of separate and combined room inputs.

    Rules applied:
    1) If kitchen>0 AND dining_room>0 AND combo_kitchen_dining>0:
       set kitchen=0 and dining_room=0; keep combo row;
       attach assumption note to combo row.
    2) If kitchen=0 AND dining_room>0 AND combo_kitchen_dining>0:
       allow; attach assumption note to combo row.
    3) If kitchen>0 AND living_room>0 AND combo_kitchen_living>0:
       set kitchen=0 and living_room=0; keep combo row;
       attach assumption note to combo row.
    4) If kitchen=0 AND living_room>0 AND combo_kitchen_living>0:
       allow; attach assumption note to combo row.

    This is handled as a non-blocking reconciliation with explicit notes and
    warnings rather than a hard validation error.
    """
    rows_by_room: dict[str, dict[str, Any]] = {
        row["room_type"]: row for row in dwelling_rows
    }

    def get_count(room_type: str) -> int:
        row = rows_by_room.get(room_type)
        if row is None:
            return 0
        return int(row.get("count", 0))

    kitchen_count  = get_count("kitchen")
    dining_count   = get_count("dining_room")
    living_count   = get_count("living_room")
    combo_kd_count = get_count("combo_kitchen_dining")
    combo_kl_count = get_count("combo_kitchen_living")

    # Cases:

    # 1) kitchen + dining_room + combo_kitchen_dining
    if kitchen_count > 0 and dining_count > 0 and combo_kd_count > 0:
        rows_by_room["kitchen"]["count"] = 0
        rows_by_room["dining_room"]["count"] = 0

        note = (
            "Separate and combined kitchens and dining rooms were inputted: "
            "Separate rooms were removed."
        )
        append_assumption_note(rows_by_room["combo_kitchen_dining"], note)

        warnings.append({
            "type": "combo_room_reconciliation_applied",
            "response_id": response_id,
            "row_index": row_index,
            "rule": "kitchen+dining_room+combo_kitchen_dining",
            "detail": note,
        })

    # 2) no kitchen + dining_room + combo_kitchen_dining
    elif kitchen_count == 0 and dining_count > 0 and combo_kd_count > 0:
        note = (
            "Separate and combined dining rooms were inputted: "
            "No changes made."
        )
        append_assumption_note(rows_by_room["combo_kitchen_dining"], note)

        warnings.append({
            "type": "combo_room_reconciliation_noted",
            "response_id": response_id,
            "row_index": row_index,
            "rule": "dining_room+combo_kitchen_dining",
            "detail": note,
        })

    # 3) kitchen + living_room + combo_kitchen_living
    if kitchen_count > 0 and living_count > 0 and combo_kl_count > 0:
        rows_by_room["kitchen"]["count"] = 0
        rows_by_room["living_room"]["count"] = 0

        note = (
            "Separate and combined kitchens and living rooms were inputted: "
            "Separate rooms were removed."
        )
        append_assumption_note(rows_by_room["combo_kitchen_living"], note)

        warnings.append({
            "type": "combo_room_reconciliation_applied",
            "response_id": response_id,
            "row_index": row_index,
            "rule": "kitchen+living_room+combo_kitchen_living",
            "detail": note,
        })

    # 4) no kitchen + living_room + combo_kitchen_living
    elif kitchen_count == 0 and living_count > 0 and combo_kl_count > 0:
        note = (
            "Separate and combined living rooms were inputted: "
            "No changes made."
        )
        append_assumption_note(rows_by_room["combo_kitchen_living"], note)

        warnings.append({
            "type": "combo_room_reconciliation_noted",
            "response_id": response_id,
            "row_index": row_index,
            "rule": "living_room+combo_kitchen_living",
            "detail": note,
        })

    return dwelling_rows


# -----------------------------------------------------------------------------
# Insert helpers
# -----------------------------------------------------------------------------

# Creates a new entry in sources for the ingest
# (This is a legacy feature from when it was assumed that data would
# come from a variety of different sources/files.)
def insert_source_row(conn: sqlite3.Connection, input_file: str | Path) -> str:
    """Insert one source record representing the current survey import event.

    A fresh UUID is used because the survey file is overwritten in place and is
    not treated as a durable file-identity object.
    """
    path = Path(input_file)
    source_id = uuid.uuid4().hex

    sql = f"""
        INSERT INTO {TABLE_SOURCES} (
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
    """
    conn.execute(
        sql,
        (
            source_id,
            SOURCE_TYPE,
            SOURCE_DESCRIPTION,
            SOURCE_ORG,
            path.name,
            str(path),
            None,
            None,
            utc_now_iso(), # From ingest_utils.py
            None,
        ),
    )
    return source_id

# Adds all candidate item rows to inventory_observations table
def insert_inventory_rows(conn: sqlite3.Connection, source_id: str, rows: list[dict[str, Any]]) -> None:
    """Bulk insert staged inventory observation rows for one survey source."""
    if not rows:
        return
    sql = f"""
        INSERT INTO {TABLE_INVENTORY} (
            response_id,
            source_id,
            room_type,
            item_name,
            count
        ) VALUES (?, ?, ?, ?, ?)
    """
    payload = [
        (r["response_id"], source_id, r["room_type"], r["item_name"], r["count"])
        for r in rows
    ]
    conn.executemany(sql, payload)

# Adds all candidate room rows to dwelling_observations table
def insert_dwelling_rows(conn: sqlite3.Connection, source_id: str, rows: list[dict[str, Any]]) -> None:
    """Bulk insert staged dwelling observation rows for one survey source."""
    if not rows:
        return
    sql = f"""
        INSERT INTO {TABLE_DWELLING} (
            response_id,
            source_id,
            room_type,
            count,
            assumption_notes
        ) VALUES (?, ?, ?, ?, ?)
    """
    payload = [
        (
            r["response_id"],
            source_id,
            r["room_type"],
            r["count"],
            r.get("assumption_notes"),
        )
        for r in rows
    ]
    conn.executemany(sql, payload)

# Adds all candidate comment rows to survey_comment table.
def insert_comment_rows(conn: sqlite3.Connection, source_id: str, rows: list[dict[str, Any]]) -> None:
    """Bulk insert staged free-text comment rows for one survey source."""
    if not rows:
        return
    sql = f"""
        INSERT INTO {TABLE_COMMENTS} (
            response_id,
            source_id,
            comment_type,
            comment_text
        ) VALUES (?, ?, ?, ?)
    """
    payload = [
        (r["response_id"], source_id, r["comment_type"], r["comment_text"])
        for r in rows
    ]
    conn.executemany(sql, payload)


# -----------------------------------------------------------------------------
# Reporting / validation helpers
# -----------------------------------------------------------------------------

# Creates a structures error dictionary
# Note: **kwargs allows the function to accept any number of keyword arguments 
def error_record(error_type: str, **kwargs: Any) -> dict[str, Any]:
    """Build a small structured error dictionary for reporting and debugging."""
    out = {"type": error_type}
    out.update(kwargs)
    return out

# List of errors that abort the ingest (--apply)
def has_blocking_errors(errors: list[dict[str, Any]]) -> bool:
    """Return True if any collected validation errors should block apply mode."""
    blocking = {
        "missing_file",
        "read_excel_failed",
        "invalid_file_structure",
        "empty_workbook",
        "unknown_section_number",
        "unmapped_item_description",
        "unmapped_room_description",
        "unexpected_count_field_in_comment_section",
        "missing_response_id",
        "invalid_count_value",
        "duplicate_response_id_in_file",
        "unexpected_text_in_section_title_column",
    }
    return any(e.get("type") in blocking for e in errors)

# Creates a structured summary of ingest plan
def summarise_plan_result(file_plan: FilePlan) -> dict[str, Any]:
    """Convert a :class:`FilePlan` into a serialisable CLI-friendly summary."""
    header_type_counts: dict[str, int] = {}
    for h in file_plan.parsed_headers:
        header_type_counts[h.header_type] = header_type_counts.get(h.header_type, 0) + 1

    grouped_errors: dict[str, int] = {}
    for e in file_plan.errors:
        et = str(e.get("type"))
        grouped_errors[et] = grouped_errors.get(et, 0) + 1

    return {
        "file": file_plan.file_name,
        "file_path": file_plan.file_path,
        "n_rows": file_plan.n_rows,
        "n_headers": file_plan.n_headers,
        "header_type_counts": header_type_counts,
        "inventory_rows": len(file_plan.inventory_rows),
        "dwelling_rows": len(file_plan.dwelling_rows),
        "comment_rows": len(file_plan.comment_rows),
        "response_ids_in_file": len(file_plan.response_ids_in_file),
        "errors": file_plan.errors,
        "error_counts": grouped_errors,
        "warnings": file_plan.warnings,
        "has_blocking_errors": file_plan.has_blocking_errors,
    }
