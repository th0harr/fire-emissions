import sqlite3
from collections import Counter

# --- user inputs ---
db_path = r"C:\Users\s9812777\University of Edinburgh\Carbon accounting of fire events - Fire-Emissions-Databases\test_db\database\pooled_inventory.sqlite"
item_name = "sofa_seats_2-3"
room_type = "living_room"


def inspect_item_count_distribution(db_path, item_name, room_type):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    query = """
        SELECT count
        FROM inventory_observations
        WHERE item_name = ?
          AND room_type = ?
    """
    cur.execute(query, (item_name, room_type))
    rows = cur.fetchall()
    conn.close()

    counts = [row[0] for row in rows]

    if not counts:
        print(f"No observations found for item_name='{item_name}' in room_type='{room_type}'.")
        return

    n = len(counts)
    freq = Counter(counts)

    print(f"\nItem:      {item_name}")
    print(f"Room type: {room_type}")
    print(f"Observations: {n}\n")

    print("Count | Frequency | Probability")
    print("-------------------------------")

    for k in range(0, 11):
        f = freq.get(k, 0)
        p = f / n
        print(f"{k:>5} | {f:>9} | {p:>10.4f}")

    # optional overflow bin
    overflow_freq = sum(v for k, v in freq.items() if k > 10)
    if overflow_freq > 0:
        overflow_prob = overflow_freq / n
        print(f"{'11+':>5} | {overflow_freq:>9} | {overflow_prob:>10.4f}")

    # expected count from raw observed data
    expected_count = sum(counts) / n

    # equivalent expected count from PMF, just to show the same result
    expected_count_pmf = sum(k * (v / n) for k, v in freq.items())

    print("\nExpected count:")
    print(f"  mean     = {expected_count:.4f}")
    print(f"  from PMF = {expected_count_pmf:.4f}")


if __name__ == "__main__":
    inspect_item_count_distribution(db_path, item_name, room_type)