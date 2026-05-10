# scripts/plot_room_carbon_stock.py
"""
Plot a boxplot-style chart for room carbon stock summaries.

Current plot contents:
    - q25 to q75 shown as the "box"
    - expected mean shown as a horizontal line and point

Included room types:
    - bedroom
    - living_room
    - kitchen

Important note:
    This is not a full statistical boxplot, because the current
    room_carbon_stock table stores:
        - mean
        - q25
        - q75
    but does not yet store:
        - median
        - min / max / whiskers

So this is best described as a quartile-band + mean plot.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle


ROOM_TYPES = ["bedroom", "living_room", "kitchen"]


def fetch_room_carbon_stock(db_path: Path) -> list[dict]:
    """
    Read room carbon stock summary values from the database.

    We currently plot the total carbon stock fields:
        - expected_total_carbon_kgC
        - q25_total_carbon_kgC
        - q75_total_carbon_kgC
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        placeholders = ", ".join("?" for _ in ROOM_TYPES)

        rows = conn.execute(f"""
            SELECT
                room_type,
                expected_total_carbon_kgC,
                q25_total_carbon_kgC,
                q75_total_carbon_kgC
            FROM room_carbon_stock
            WHERE room_type IN ({placeholders})
            ORDER BY CASE room_type
                WHEN 'bedroom' THEN 1
                WHEN 'living_room' THEN 2
                WHEN 'kitchen' THEN 3
                ELSE 99
            END
        """, ROOM_TYPES).fetchall()

        return [
            {
                "room_type": row["room_type"],
                "mean": row["expected_total_carbon_kgC"],
                "q25": row["q25_total_carbon_kgC"],
                "q75": row["q75_total_carbon_kgC"],
            }
            for row in rows
        ]

    finally:
        conn.close()


def plot_room_carbon_stock_boxplot_style(
    db_path: Path,
    output_path: Path | None = None,
) -> None:
    """
    Make a boxplot-style chart from room_carbon_stock.

    Visual encoding:
        - rectangle from q25 to q75
        - horizontal line at the mean
        - point at the mean for visibility
    """
    rows = fetch_room_carbon_stock(db_path)

    if not rows:
        raise RuntimeError(
            "No room_carbon_stock rows found for bedroom, living_room, or kitchen."
        )

    fig, ax = plt.subplots(figsize=(8, 6))

    box_width = 0.6

    for i, row in enumerate(rows, start=1):
        room_type = row["room_type"]
        mean_val = row["mean"]
        q25_val = row["q25"]
        q75_val = row["q75"]

        if mean_val is None or q25_val is None or q75_val is None:
            continue

        mean_val = float(mean_val)
        q25_val = float(q25_val)
        q75_val = float(q75_val)

        # Draw the interquartile "box" from q25 to q75.
        rect = Rectangle(
            (i - box_width / 2, q25_val),
            box_width,
            q75_val - q25_val,
            fill=False,
            linewidth=1.5,
        )
        ax.add_patch(rect)

        # Draw a horizontal line across the box at the mean.
        ax.hlines(
            y=mean_val,
            xmin=i - box_width / 2,
            xmax=i + box_width / 2,
            linewidth=1.5,
        )

        # Add a point marker at the mean for clarity.
        ax.plot(i, mean_val, marker="o")

        # Optional: annotate values.
        ax.text(i, q75_val, f"{q75_val:.1f}", ha="center", va="bottom", fontsize=9)
        ax.text(i, mean_val, f"{mean_val:.1f}", ha="center", va="bottom", fontsize=9)
        ax.text(i, q25_val, f"{q25_val:.1f}", ha="center", va="top", fontsize=9)

    ax.set_xticks(range(1, len(rows) + 1))
    ax.set_xticklabels([row["room_type"] for row in rows])

    ax.set_ylabel("Total carbon stock (kgC)")
    ax.set_title("Room carbon stock summary (q25 - q75 with mean)")
    ax.set_xlim(0.5, len(rows) + 0.5)

    ax.grid(True, axis="y")
    fig.tight_layout()

    if output_path is not None:
        fig.savefig(output_path, dpi=300, bbox_inches="tight")
        print(f"Saved figure to: {output_path}")

    plt.show()


if __name__ == "__main__":
    # Update this path as needed for the local DB location.
    db_path = Path(r"C:\Users\s9812777\University of Edinburgh\Carbon accounting of fire events - Fire-Emissions-Databases\test_db\database\pooled_inventory.sqlite")

    # Optional output file path.
    output_path = Path("room_carbon_stock_boxplot.png")

    plot_room_carbon_stock_boxplot_style(
        db_path=db_path,
        output_path=output_path,
    )