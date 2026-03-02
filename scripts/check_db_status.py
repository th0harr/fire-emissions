import sqlite3
from pathlib import Path

db = Path("data/processed/pooled_inventory.sqlite")  # adjust if your path differs
print("DB exists:", db.exists())
if not db.exists():
    raise SystemExit(f"No database file found at: {db.resolve()}")

print("DB path:", db.resolve())
print("DB size (bytes):", db.stat().st_size)

con = sqlite3.connect(db)
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

con.close()
