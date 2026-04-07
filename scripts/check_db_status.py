from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from scripts.ingest import load_local_paths_config


def resolve_db_path(profile: str, db_handle: str, config: dict) -> Path:
    """
    Resolve the full SQLite database path for the selected profile and db handle.
    """
    profiles = config.get("profiles", {})
    db_roots = config.get("db_roots", {})

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

    sharepoint_root = Path(profiles[profile]["sharepoint_root"])
    db_cfg = db_roots[db_handle]

    root = db_cfg.get("root")
    rel_db = db_cfg.get("rel_db")

    if not root:
        raise KeyError(f"Missing required db_roots.{db_handle}.root in config.")
    if not rel_db:
        raise KeyError(f"Missing required db_roots.{db_handle}.rel_db in config.")

    return sharepoint_root / Path(root) / Path(rel_db)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="check_db_status",
        description="Check whether a Fire Emissions SQLite database exists and inspect its contents.",
    )
    parser.add_argument(
        "--profile",
        required=True,
        help="Profile name from config/local_paths.yaml (e.g. tom).",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="Database handle from config/local_paths.yaml (e.g. inventory_db, test_db, fire_db).",
    )
    args = parser.parse_args(argv)

    config = load_local_paths_config(Path("config") / "local_paths.yaml")
    db = resolve_db_path(args.profile, args.db, config)

    print("DB exists:", db.exists())
    if not db.exists():
        raise SystemExit(f"No database file found at: {db.resolve()}")

    print("DB path:", db.resolve())
    print("DB size (bytes):", db.stat().st_size)

    con = sqlite3.connect(db)
    try:
        cur = con.cursor()

        cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        tables = [r[0] for r in cur.fetchall()]

        print("\nTables:")
        for t in tables:
            print(" ", t)

        print("\nRow counts:")
        for t in tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {t};")
                n = cur.fetchone()[0]
                print(f"  {t}: {n}")
            except Exception as e:
                print(f"  {t}: (could not count) {e}")

        for table_name in ["item_dictionary", "furniture", "room"]:
            if table_name in tables:
                print(f"\nSchema for {table_name}:")
                cur.execute(f"PRAGMA table_info({table_name});")
                for row in cur.fetchall():
                    print(" ", row)

    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())