# scripts/assumed_items.py
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List

import pandas as pd


# -------------------------------------
# ASSUMED INVENTORY ROW STRUCTURE
# Defines the expected in-memory row format
# -------------------------------------

@dataclass(frozen=True)
class AssumedItemRow:
    """
    Represents one validated row from assumed_items.xlsx.

    Notes:
    - item_name does NOT need to be unique across the file.
    - The combination of item_name + room_type must be unique.
    - Dependency calculations are NOT performed here.
      This module only validates and stores the assumptions.
    """

    item_name: str
    room_type: str
    count_assumed: int
    dependency: Optional[str]
    dependency_type: Optional[str]
    dependency_quantifier: Optional[float]
    assumption_notes: Optional[str]


# -------------------------------------
# PRIVATE HELPER FUNCTIONS
# Small validation/normalisation helpers
# -------------------------------------

def _require_cols(df: pd.DataFrame, required: list[str], sheet: str) -> None:
    """
    Checks that the DataFrame contains all required columns.

    Raises:
        ValueError if any required column is missing.
    """

    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(
            f"Sheet '{sheet}' missing required columns: {missing}. "
            f"Found: {list(df.columns)}"
        )


def _normalise_required_text(value, *, field_name: str) -> str:
    """
    Normalises required text identifiers.

    Used for:
    - item_name
    - room_type

    These fields must be present and non-blank.
    """

    if value is None or pd.isna(value):
        raise ValueError(f"[assumed_inventory] Required field '{field_name}' is blank.")

    text = str(value).strip().lower()

    if text in {"", "none", "nan"}:
        raise ValueError(f"[assumed_inventory] Required field '{field_name}' is blank.")

    return text


def _normalise_optional_text(value) -> Optional[str]:
    """
    Normalises optional text identifiers.

    Used for:
    - dependency
    - dependency_type

    Returns:
        None for blank / missing values.
    """

    if value is None or pd.isna(value):
        return None

    text = str(value).strip().lower()

    if text in {"", "none", "nan"}:
        return None

    return text


def _normalise_optional_note(value) -> Optional[str]:
    """
    Normalises optional free-text notes.

    Unlike item_name / room_type / dependency values, notes are not lower-cased.
    """

    if value is None or pd.isna(value):
        return None

    text = str(value).strip()

    if text in {"", "None", "none", "nan", "NaN"}:
        return None

    return text


def _load_reference_sets(db_path: str | Path) -> tuple[set[str], set[str]]:
    """
    Loads valid item_name and room_type values from the database.

    These are used to validate:
    - assumed_inventory.item_name
    - assumed_inventory.room_type
    - dependency values when dependency_type is item_name or room_type
    """

    db_path = Path(db_path)

    con = sqlite3.connect(str(db_path))

    try:
        cur = con.cursor()

        item_names = {
            str(row[0]).strip().lower()
            for row in cur.execute(
                """
                SELECT item_name
                FROM item_dictionary;
                """
            ).fetchall()
        }

        room_types = {
            str(row[0]).strip().lower()
            for row in cur.execute(
                """
                SELECT room_type
                FROM room;
                """
            ).fetchall()
        }

        return item_names, room_types

    finally:
        con.close()


# -------------------------------------
# MAIN READ + VALIDATE FUNCTION
# Reads assumed_items.xlsx and validates rows
# -------------------------------------

def read_assumed_items_xlsx_pandas(
    *,
    db_path: str | Path,
    xlsx_path: str | Path,
) -> List[AssumedItemRow]:
    """
    Reads and validates assumed_items.xlsx.

    Expected columns:
    - item_name
    - room_type
    - count_assumed
    - dependency
    - dependency_type
    - dependency_quantifier
    - assumption_notes

    Validation rules:
    1. All expected columns must be present.
    2. item_name must exist in item_dictionary.item_name.
    3. room_type must exist in room.room_type.
    4. item_name + room_type must be unique.
    5. count_assumed must be a non-negative integer.
    6. dependency, dependency_type and dependency_quantifier may all be blank.
    7. If any dependency field is present, all three dependency fields must be present.
    8. dependency_type must be one of:
       - item_name
       - room_type
    9. If dependency_type == item_name, dependency must exist in item_dictionary.item_name.
    10. If dependency_type == room_type, dependency must exist in room.room_type.

    Important:
    - No dependency calculations are performed here.
    - This function only validates the assumption file and prepares rows for insertion.
    """

    db_path = Path(db_path)
    xlsx_path = Path(xlsx_path)

    # -------------------------------------
    # Load assumed_items.xlsx
    # -------------------------------------

    df = pd.read_excel(xlsx_path, engine="openpyxl")

    # Strip any accidental whitespace from Excel column headers.
    df.columns = [str(c).strip() for c in df.columns]

    # -------------------------------------
    # Validate expected columns
    # -------------------------------------

    required_cols = [
        "item_name",
        "room_type",
        "count_assumed",
        "dependency",
        "dependency_type",
        "dependency_quantifier",
        "assumption_notes",
    ]

    _require_cols(df, required_cols, "assumed_items")

    # Restrict to the expected columns.
    # This avoids accidentally ingesting hidden / temporary Excel columns.
    df = df[required_cols].copy()

    # Drop rows where all expected fields are blank.
    # This allows a little breathing room at the bottom of the spreadsheet.
    df = df.dropna(how="all")

    # -------------------------------------
    # Normalise required identifier columns
    # -------------------------------------

    df["item_name"] = [
        _normalise_required_text(v, field_name="item_name")
        for v in df["item_name"]
    ]

    df["room_type"] = [
        _normalise_required_text(v, field_name="room_type")
        for v in df["room_type"]
    ]

    # -------------------------------------
    # Normalise optional dependency columns
    # -------------------------------------

    df["dependency"] = [
        _normalise_optional_text(v)
        for v in df["dependency"]
    ]

    df["dependency_type"] = [
        _normalise_optional_text(v)
        for v in df["dependency_type"]
    ]

    df["assumption_notes"] = [
        _normalise_optional_note(v)
        for v in df["assumption_notes"]
    ]

    # -------------------------------------
    # Validate and normalise count_assumed
    # -------------------------------------

    df["count_assumed"] = pd.to_numeric(
        df["count_assumed"],
        errors="raise",
    )

    bad_count = df.loc[
        (df["count_assumed"] < 0)
        | (df["count_assumed"] % 1 != 0),
        ["item_name", "room_type", "count_assumed"],
    ]

    if not bad_count.empty:
        raise ValueError(
            "[assumed_inventory] count_assumed must be a non-negative integer. "
            f"Bad rows: {bad_count.to_dict(orient='records')[:20]}"
        )

    df["count_assumed"] = df["count_assumed"].astype(int)

    # -------------------------------------
    # Validate and normalise dependency_quantifier
    # -------------------------------------

    df["dependency_quantifier"] = pd.to_numeric(
        df["dependency_quantifier"],
        errors="coerce",
    )

    bad_quantifier = df.loc[
        df["dependency_quantifier"].notna()
        & (df["dependency_quantifier"] < 0),
        ["item_name", "room_type", "dependency_quantifier"],
    ]

    if not bad_quantifier.empty:
        raise ValueError(
            "[assumed_inventory] dependency_quantifier must be >= 0. "
            f"Bad rows: {bad_quantifier.to_dict(orient='records')[:20]}"
        )

    # -------------------------------------
    # Validate uniqueness of item_name + room_type
    # -------------------------------------

    duplicated_pairs = df.loc[
        df.duplicated(subset=["item_name", "room_type"], keep=False),
        ["item_name", "room_type"],
    ]

    if not duplicated_pairs.empty:
        raise ValueError(
            "[assumed_inventory] The combination of item_name + room_type must be unique. "
            "Multiple assumed instances should be recorded using count_assumed, not duplicate rows. "
            f"Duplicate pairs: {duplicated_pairs.drop_duplicates().to_dict(orient='records')[:20]}"
        )

    # -------------------------------------
    # Load database vocab references
    # -------------------------------------

    valid_items, valid_rooms = _load_reference_sets(db_path)

    # -------------------------------------
    # Validate item_name against item_dictionary
    # -------------------------------------

    missing_items = sorted(set(df["item_name"]) - valid_items)

    if missing_items:
        raise ValueError(
            "[assumed_inventory] item_name values not present in item_dictionary: "
            + ", ".join(missing_items[:20])
        )

    # -------------------------------------
    # Validate room_type against room table
    # -------------------------------------

    missing_rooms = sorted(set(df["room_type"]) - valid_rooms)

    if missing_rooms:
        raise ValueError(
            "[assumed_inventory] room_type values not present in room table: "
            + ", ".join(missing_rooms[:20])
        )

    # -------------------------------------
    # Validate dependency fields as a linked group
    # -------------------------------------

    has_dependency = df["dependency"].notna()
    has_dependency_type = df["dependency_type"].notna()
    has_dependency_quantifier = df["dependency_quantifier"].notna()

    incomplete_dependency_rows = df.loc[
        has_dependency | has_dependency_type | has_dependency_quantifier
    ].loc[
        ~(has_dependency & has_dependency_type & has_dependency_quantifier),
        [
            "item_name",
            "room_type",
            "dependency",
            "dependency_type",
            "dependency_quantifier",
        ],
    ]

    if not incomplete_dependency_rows.empty:
        raise ValueError(
            "[assumed_inventory] dependency, dependency_type and dependency_quantifier "
            "must either all be blank or all be provided. "
            f"Bad rows: {incomplete_dependency_rows.to_dict(orient='records')[:20]}"
        )

    # -------------------------------------
    # Validate dependency_type allowed values
    # -------------------------------------

    valid_dependency_types = {"item_name", "room_type"}

    bad_dependency_types = sorted(
        set(df.loc[df["dependency_type"].notna(), "dependency_type"])
        - valid_dependency_types
    )

    if bad_dependency_types:
        raise ValueError(
            "[assumed_inventory] dependency_type must be one of "
            f"{sorted(valid_dependency_types)}. "
            f"Bad values: {bad_dependency_types[:20]}"
        )

    # -------------------------------------
    # Validate dependency values against the correct vocab set
    # -------------------------------------

    item_dependency_values = set(
        df.loc[df["dependency_type"] == "item_name", "dependency"].dropna()
    )

    missing_item_dependencies = sorted(item_dependency_values - valid_items)

    if missing_item_dependencies:
        raise ValueError(
            "[assumed_inventory] dependency values with dependency_type='item_name' "
            "must exist in item_dictionary. Missing: "
            + ", ".join(missing_item_dependencies[:20])
        )

    room_dependency_values = set(
        df.loc[df["dependency_type"] == "room_type", "dependency"].dropna()
    )

    missing_room_dependencies = sorted(room_dependency_values - valid_rooms)

    if missing_room_dependencies:
        raise ValueError(
            "[assumed_inventory] dependency values with dependency_type='room_type' "
            "must exist in room. Missing: "
            + ", ".join(missing_room_dependencies[:20])
        )

    # -------------------------------------
    # Convert validated DataFrame rows to dataclasses
    # -------------------------------------

    assumed_items = [
        AssumedItemRow(
            item_name=r.item_name,
            room_type=r.room_type,
            count_assumed=int(r.count_assumed),
            dependency=None if pd.isna(r.dependency) else r.dependency,
            dependency_type=None if pd.isna(r.dependency_type) else r.dependency_type,
            dependency_quantifier=None
            if pd.isna(r.dependency_quantifier)
            else float(r.dependency_quantifier),
            assumption_notes=None
            if pd.isna(r.assumption_notes)
            else r.assumption_notes,
        )
        for r in df.itertuples(index=False)
    ]

    return assumed_items


# -------------------------------------
# MAIN DATABASE INGEST FUNCTION
# Inserts validated assumed inventory rows into SQLite
# -------------------------------------

def ingest_assumed_items_pandas(
    *,
    db_path: str | Path,
    xlsx_path: str | Path,
    mode: str = "replace_all",
) -> None:
    """
    Ingests assumed_items.xlsx into the assumed_inventory table.

    Current policy:
    - replace_all:
        DELETE all existing assumed_inventory rows,
        then INSERT all validated rows from assumed_items.xlsx.

    Notes:
    - This mirrors the controlled-vocab ingest pattern.
    - assumed_items.xlsx is treated as the canonical source file.
    - No model calculations are performed here.
    """

    db_path = Path(db_path)
    xlsx_path = Path(xlsx_path)

    # -------------------------------------
    # Read and validate the workbook first
    # -------------------------------------

    assumed_items = read_assumed_items_xlsx_pandas(
        db_path=db_path,
        xlsx_path=xlsx_path,
    )

    # -------------------------------------
    # Open SQLite connection and begin transaction
    # -------------------------------------

    con = sqlite3.connect(str(db_path))

    try:
        cur = con.cursor()

        # Ensure SQLite enforces foreign key constraints during ingest.
        cur.execute("PRAGMA foreign_keys = ON;")

        # Start transaction explicitly so the ingest either fully succeeds or fully rolls back.
        cur.execute("BEGIN;")

        # -------------------------------------
        # Clear existing assumed inventory rows
        # -------------------------------------

        if mode == "replace_all":
            cur.execute("DELETE FROM assumed_inventory;")

        elif mode != "upsert":
            raise ValueError("mode must be 'replace_all' or 'upsert'")

        # -------------------------------------
        # Insert validated assumed inventory rows
        # -------------------------------------

        for r in assumed_items:
            cur.execute(
                """
                INSERT OR REPLACE INTO assumed_inventory
                (room_type, item_name, count_assumed,
                 dependency, dependency_type, dependency_quantifier,
                 assumption_notes)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    r.room_type,
                    r.item_name,
                    r.count_assumed,
                    r.dependency,
                    r.dependency_type,
                    r.dependency_quantifier,
                    r.assumption_notes,
                ),
            )

        # Commit only once all rows have inserted successfully.
        con.commit()

        print(
            "assumed_items ingest complete:",
            f"{len(assumed_items)} assumed inventory rows; mode={mode}"
        )

    # Roll back on any error, so the table is never left partially updated.
    except Exception:
        con.rollback()
        raise

    finally:
        con.close()