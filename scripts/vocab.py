from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd

# Create classes
@dataclass(frozen=True)
class ItemRow:
    item_name: str
    item_description: str
    item_mass: float
    furniture_class: str
    notes: Optional[str]

@dataclass(frozen=True)
class ClassRow:
    furniture_class: str
    furniture_description: str
    class_contains: str
    kgC_kg: float
    ratio_fossil: Optional[float]   # nullable
    ratio_biog: Optional[float]     # nullable
    notes: Optional[str]

@dataclass(frozen=True)
class RoomRow:
    room_type: str
    notes: Optional[str]

# Private function
def _require_cols(df: pd.DataFrame, required: list[str], sheet: str) -> None:
    """Checks the DataFrame contains the required columns"""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Sheet '{sheet}' missing required columns: {missing}. Found: {list(df.columns)}")


def read_mapping_list_xlsx_pandas(xlsx_path: str | Path) -> Tuple[List[ItemRow], List[ClassRow], List[RoomRow]]:
    """
    Reads and validates the mapping_list.
    Ensures the data is suitable for ingestion.
    Input: xlsx file
    Returns: validated data {items, classes, rooms}
    """
    xlsx_path = Path(xlsx_path)

    # ---- item_name ----
    # Loads the "item_name" sheet into a DataFrame
    df_items = pd.read_excel(xlsx_path, sheet_name="item_name", engine="openpyxl")
    # Strips header whitespace
    df_items.columns = [str(c).strip() for c in df_items.columns]

    # Ensures headers exist
    req_items = ["item_name", "item_description", "item_mass", "furniture_class"]
    _require_cols(df_items, req_items, "item_name")

    # Drop rows missing required fields
    df_items = df_items.dropna(subset=req_items)

    # Normalise key id strings (prevent duplication on lower/upper-case mismatch)
    df_items["item_name"] = df_items["item_name"].astype(str).str.strip().str.lower()
    df_items["item_description"] = df_items["item_description"].astype(str).str.strip()
    df_items["furniture_class"] = df_items["furniture_class"].astype(str).str.strip().str.lower()

    # Ensure numeric
    df_items["item_mass"] = pd.to_numeric(df_items["item_mass"], errors="raise")

    # Notes optional
    if "notes" not in df_items.columns:
        df_items["notes"] = None
    else:
        df_items["notes"] = df_items["notes"].astype(str).where(df_items["notes"].notna(), None)

    # Ensure item_name (primary key) is unique
    if df_items["item_name"].duplicated().any():
        dups = df_items.loc[df_items["item_name"].duplicated(), "item_name"].tolist()
        raise ValueError(f"[item_name] Duplicate item_name(s): {dups[:20]}")

    # Ensure positive mass
    if (df_items["item_mass"] <= 0).any():
        bad = df_items.loc[df_items["item_mass"] <= 0, "item_name"].tolist()
        raise ValueError(f"[item_name] item_mass must be > 0 for: {bad[:20]}")

    # Convert to dataclasses (and detaches from pandas types)
    items = [
        ItemRow(r.item_name, r.item_description, float(r.item_mass), r.furniture_class, None if r.notes in ("None", "nan") else r.notes)
        for r in df_items.itertuples(index=False)
    ]

    # ---- furniture_class ----
    # Loads the "furniture_class" sheet into a DataFrame
    df_cls = pd.read_excel(xlsx_path, sheet_name="furniture_class", engine="openpyxl")
    # Strips header whitespace
    df_cls.columns = [str(c).strip() for c in df_cls.columns]

    # Ensures headers exist
    req_cls = ["furniture_class", "furniture_description", "class_contains", "kgC_kg", "ratio_fossil", "ratio_biog"]
    _require_cols(df_cls, req_cls, "furniture_class")
    
    # Drop rows missing required fields
    # (ratios are allowed to be blank but columns must exist)
    df_cls = df_cls.dropna(subset=["furniture_class", "furniture_description", "class_contains", "kgC_kg"])

    # Normalise key id strings (prevent duplication on lower/upper-case mismatch)
    df_cls["furniture_class"] = df_cls["furniture_class"].astype(str).str.strip().str.lower()
    df_cls["furniture_description"] = df_cls["furniture_description"].astype(str).str.strip()
    df_cls["class_contains"] = df_cls["class_contains"].astype(str).str.strip()
    
    # Ensure numeric values
    df_cls["kgC_kg"] = pd.to_numeric(df_cls["kgC_kg"], errors="raise")
    
    # Ratios can be blank but values must be numeric
    df_cls["ratio_fossil"] = pd.to_numeric(df_cls["ratio_fossil"], errors="coerce")
    df_cls["ratio_biog"] = pd.to_numeric(df_cls["ratio_biog"], errors="coerce")

    # Notes optional
    if "notes" not in df_cls.columns:
        df_cls["notes"] = None
    else:
        df_cls["notes"] = df_cls["notes"].astype(str).where(df_cls["notes"].notna(), None)

    # Ensure furniture_class (primary key) is unique
    if df_cls["furniture_class"].duplicated().any():
        dups = df_cls.loc[df_cls["furniture_class"].duplicated(), "furniture_class"].tolist()
        raise ValueError(f"[furniture_class] Duplicate furniture_class(es): {dups[:20]}")

    # Ensure carbon:item ratio is between 0-1
    if ((df_cls["kgC_kg"] <= 0) | (df_cls["kgC_kg"] >= 1)).any():
        bad = df_cls.loc[(df_cls["kgC_kg"] <= 0) | (df_cls["kgC_kg"] >= 1), "furniture_class"].tolist()
        raise ValueError(f"[furniture_class] kgC_kg must be in (0,1) for: {bad[:20]}")

    # Internal function to identify optional floats
    def _maybe_float(v) -> Optional[float]:
        if v is None:
            return None
        # pandas uses NaN for blanks
        if pd.isna(v):
            return None
        # We assume workbook is controlled: if not blank, it's numeric
        return float(v)

    # Validate ratio ranges only when present (not NaN)
    rf = df_cls["ratio_fossil"].apply(_maybe_float)
    rb = df_cls["ratio_biog"].apply(_maybe_float)

    # Ensure carbon source ratios are from 0-1 (if present)
    bad_rf = df_cls.loc[rf.notna() & ((rf < 0) | (rf > 1)), "furniture_class"].tolist()
    if bad_rf:
        raise ValueError(f"[furniture_class] ratio_fossil must be in [0,1] for: {bad_rf[:20]}")

    bad_rb = df_cls.loc[rb.notna() & ((rb < 0) | (rb > 1)), "furniture_class"].tolist()
    if bad_rb:
        raise ValueError(f"[furniture_class] ratio_biog must be in [0,1] for: {bad_rb[:20]}")

    # Ensure ratio_fossil + ratio_biog = 1.0 (1 dp rounding)
    both = rf.notna() & rb.notna()
    sum_bad = df_cls.loc[both & ((rf + rb - 1.0).abs() > 1e-6), "furniture_class"].tolist()
    if sum_bad:
        raise ValueError(
            "[furniture_class] ratio_fossil + ratio_biog must equal 1.0 for: "
            + ", ".join(sum_bad[:20])
        )

    # Build ClassRow objects using rf/rb (as Python floats or None)
    classes = [
        ClassRow(
            furniture_class=str(r.furniture_class).strip().lower(),
            furniture_description=str(r.furniture_description).strip(),
            class_contains=str(r.class_contains).strip(),
            kgC_kg=float(r.kgC_kg),
            ratio_fossil=rf.iloc[idx],
            ratio_biog=rb.iloc[idx],
            notes=None if (r.notes is None or str(r.notes).strip() == "") else str(r.notes).strip(),
        )
        for idx, r in enumerate(df_cls.itertuples(index=False))
    ]
    
    # ---- room_type ----
    # Loads the "room_type" sheet into a DataFrame
    df_rooms = pd.read_excel(xlsx_path, sheet_name="room_type", engine="openpyxl")
    # Strips header whitespace
    df_rooms.columns = [str(c).strip() for c in df_rooms.columns]

    # Ensures headers exist
    if "room_type" not in df_rooms.columns:
        raise ValueError("Sheet 'room_type' must contain a 'room_type' column.")
    # Drop rows missing required fields
    df_rooms = df_rooms.dropna(subset=["room_type"])
    
    # Normalise key id strings
    df_rooms["room_type"] = df_rooms["room_type"].astype(str).str.strip().str.lower()

    # Notes optional
    if "notes" not in df_rooms.columns:
        df_rooms["notes"] = None
    else:
        df_rooms["notes"] = df_rooms["notes"].astype(str).where(df_rooms["notes"].notna(), None)

    # Ensure room_type (primary key) is unique
    if df_rooms["room_type"].duplicated().any():
        dups = df_rooms.loc[df_rooms["room_type"].duplicated(), "room_type"].tolist()
        raise ValueError(f"[room_type] Duplicate room_type(s): {dups[:20]}")

    # Convert to dataclasses
    rooms = [
        RoomRow(r.room_type, None if r.notes in ("None", "nan") else r.notes)
        for r in df_rooms.itertuples(index=False)
    ]

    # Ensure every item's furniture_class exists in class table
    class_set = {c.furniture_class for c in classes}
    missing = sorted({it.furniture_class for it in items if it.furniture_class not in class_set})
    if missing:
        raise ValueError("Items reference furniture_class not present in furniture_class sheet: " + ", ".join(missing[:20]))

    return items, classes, rooms


def ingest_mapping_list_pandas(
    *,
    db_path: str | Path,
    xlsx_path: str | Path,
    mode: str = "replace_all",  # "replace_all" or "upsert"
) -> None:
    items, classes, rooms = read_mapping_list_xlsx_pandas(xlsx_path)
    """ Ingests the validated data """
    
    # Opens the DB, starts a transaction
    con = sqlite3.connect(str(db_path))
    try:
        cur = con.cursor()
        cur.execute("PRAGMA foreign_keys = ON;")
        cur.execute("BEGIN;")

        # Wipes the current vocab tables before inserting
        if mode == "replace_all":
            cur.execute("DELETE FROM item_dictionary;")
            cur.execute("DELETE FROM furniture_class;")
            cur.execute("DELETE FROM room_type;")
        elif mode != "upsert":
            raise ValueError("mode must be 'replace_all' or 'upsert'")

        # "upsert" mode: update or insert into existing data (no wipe)
        # (replace if PK already exists, or insert of missing)
        
        # Perform on furniture_class first...
        # (as item_dictionary has a furniture_class column)
        for c in classes:
            cur.execute(
                """
                INSERT OR REPLACE INTO furniture_class
                (furniture_class, furniture_description, class_contains, kgC_kg, ratio_fossil, ratio_biog, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (c.furniture_class, c.furniture_description, c.class_contains, c.kgC_kg, c.ratio_fossil, c.ratio_biog, c.notes),
            )

        # Then items...
        for it in items:
            cur.execute(
                """
                INSERT OR REPLACE INTO item_dictionary
                (item_name, item_description, item_mass, furniture_class, notes)
                VALUES (?, ?, ?, ?, ?);
                """,
                (it.item_name, it.item_description, it.item_mass, it.furniture_class, it.notes),
            )

        # And finally rooms...
        # (Rooms are independent so can be last)
        for r in rooms:
            cur.execute(
                """
                INSERT OR REPLACE INTO room_type
                (room_type, notes)
                VALUES (?, ?);
                """,
                (r.room_type, r.notes),
            )

        con.commit()
        print("mapping_list ingest complete:",
              f"{len(classes)} furniture_class, {len(items)} items, {len(rooms)} rooms; mode={mode}")

    # Commit on success, rollback on any error
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()
