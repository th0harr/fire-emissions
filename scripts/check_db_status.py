from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from scripts.ingest import load_local_paths_config, resolve_paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="check_db_status",
        description="Check whether the Fire Emissions SQLite database exists and inspect its contents.",
    )
    parser.add_argument(
        "--profile",
        required=True,
        help="Profile name from config/local_paths.yaml (e.g. tom, tom_test).",
    )
    args = parser.parse_args(argv)

    config = load_local_paths_config(Path("config") / "local_paths.yaml")
    resolved = resolve_paths(args.profile, "vocab", config)
    db = resolved.db_path

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

        # Helpful schema checks for recently renamed vocab tables
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