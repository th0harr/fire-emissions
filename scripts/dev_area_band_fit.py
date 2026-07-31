from pathlib import Path
import sqlite3

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


DB_PATH = Path(
    r"C:\Users\s9812777\University of Edinburgh\Carbon accounting of fire events - Fire-Emissions-Databases\fire_db\database\fire_incidents.sqlite"
)

OUTPUT_DIR = Path.cwd() / "outputs" / "figures" / "area_band_fit_outputs"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------
# FIRE DAMAGE AREA BAND DEFINITIONS
# Controlled model-facing fire_events labels.
# "None" is excluded from curve fitting.
# -------------------------------------------------

AREA_BANDS = {
    1: ("None", 0.0, 0.0),
    2: ("Up to 5", 0.0, 5.0),
    3: ("6 – 10", 6.0, 10.0),
    4: ("11 – 20", 11.0, 20.0),
    5: ("21 – 50", 21.0, 50.0),
    6: ("51 – 100", 51.0, 100.0),
    7: ("101 – 200", 101.0, 200.0),
    8: ("201 – 500", 201.0, 500.0),
    9: ("501 – 1,000", 501.0, 1000.0),
    10: ("1,001 – 2,000", 1001.0, 2000.0),
    11: ("2,001 – 5,000", 2001.0, 5000.0),
    12: ("5,001 – 10,000", 5001.0, 10000.0),
}


# -------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------

def q_position(lower: float, upper: float, q: float) -> float:
    """
    Return the position at quantile q within a closed interval [lower, upper].

    For example:
        q = 0.25 -> q25
        q = 0.33 -> q33
        q = 0.50 -> midpoint / q50
        q = 0.75 -> q75
    """
    return lower + q * (upper - lower)


def within_band_median_power_law(lower: float, upper: float, exponent_b: float) -> float:
    """
    Median position within [lower, upper] for fitted density f(x) = a * x^(-b).

    This estimates the within-band location at which half of the fitted
    incident density lies below and half above, under a power-law density.
    """
    lower = max(lower, 1e-6)

    if abs(exponent_b - 1.0) < 1e-9:
        return np.sqrt(lower * upper)

    power = 1.0 - exponent_b
    return (0.5 * (upper ** power + lower ** power)) ** (1.0 / power)


def power_law_density(area_m2: np.ndarray, a: float, b: float) -> np.ndarray:
    """
    Evaluate fitted power-law density:
        density = a * area^(-b)
    """
    area_m2 = np.asarray(area_m2, dtype=float)
    area_m2 = np.maximum(area_m2, 1e-6)
    return a * np.power(area_m2, -b)


# -------------------------------------------------
# LOAD HISTOGRAM COUNTS FROM fire_events
# -------------------------------------------------

with sqlite3.connect(DB_PATH) as conn:
    df = pd.read_sql_query(
        """
        SELECT
            building_fire_damage_area_band_index AS band_index,
            building_fire_damage_area_input AS band_label,
            COUNT(*) AS n_events
        FROM fire_events
        WHERE building_fire_damage_area_band_index IS NOT NULL
          AND building_fire_damage_area_band_index >= 2
          AND building_fire_damage_area_band_index <= 12
        GROUP BY
            building_fire_damage_area_band_index,
            building_fire_damage_area_input
        ORDER BY building_fire_damage_area_band_index;
        """,
        conn,
    )


# -------------------------------------------------
# JOIN BAND BOUNDS / DERIVE BAND METRICS
# -------------------------------------------------

bounds = pd.DataFrame(
    [
        {
            "band_index": band_index,
            "canonical_label": label,
            "lower_m2": lower,
            "upper_m2": upper,
        }
        for band_index, (label, lower, upper) in AREA_BANDS.items()
    ]
)

df = df.merge(bounds, on="band_index", how="left")

df["band_width_m2"] = df["upper_m2"] - df["lower_m2"]
df["midpoint_m2"] = 0.5 * (df["lower_m2"] + df["upper_m2"])
df["q33_m2"] = df.apply(
    lambda row: q_position(row["lower_m2"], row["upper_m2"], 0.33),
    axis=1,
)

# Convert count to density because the bands have unequal widths.
df["event_density_per_m2"] = df["n_events"] / df["band_width_m2"]


# -------------------------------------------------
# FIT POWER-LAW TO DENSITY VS MIDPOINT
# log(density) = log(a) - b*log(area)
# -------------------------------------------------

fit_df = df[
    (df["band_width_m2"] > 0)
    & (df["event_density_per_m2"] > 0)
    & np.isfinite(df["midpoint_m2"])
].copy()

x_log = np.log(fit_df["midpoint_m2"].to_numpy())
y_log = np.log(fit_df["event_density_per_m2"].to_numpy())

slope_power, intercept_power = np.polyfit(x_log, y_log, deg=1)
power_b = -slope_power
power_a = np.exp(intercept_power)


# -------------------------------------------------
# ESTIMATE WITHIN-BAND POWER-LAW MEDIAN POSITIONS
# Also record q33 representative position for comparison.
# -------------------------------------------------

rows = []

for _, row in df.iterrows():
    lower = row["lower_m2"]
    upper = row["upper_m2"]

    if not np.isfinite(lower) or not np.isfinite(upper) or upper <= lower:
        continue

    median_power = within_band_median_power_law(lower, upper, power_b)
    q33_value = q_position(lower, upper, 0.33)

    rows.append({
        "band_index": row["band_index"],
        "band_label": row["canonical_label"],
        "n_events": row["n_events"],
        "lower_m2": lower,
        "upper_m2": upper,
        "midpoint_m2": row["midpoint_m2"],
        "q33_m2": q33_value,
        "density_per_m2": row["event_density_per_m2"],
        "power_law_median_m2": median_power,
        "power_law_band_position": (median_power - lower) / (upper - lower),
        "q33_band_position": 0.33,
    })

out = pd.DataFrame(rows)


# -------------------------------------------------
# TERMINAL OUTPUT
# -------------------------------------------------

print("\nHistogram / density by fire damage band")
print(df.to_string(index=False))

print("\nPower-law fit")
print(f"density = {power_a:.6g} * area^(-{power_b:.6g})")

print("\nEstimated within-band positions")
print(out.to_string(index=False))


# -------------------------------------------------
# WRITE CSV OUTPUT
# -------------------------------------------------

csv_path = OUTPUT_DIR / "fire_damage_band_within_band_median_estimates.csv"
out.to_csv(csv_path, index=False)
print(f"\nWrote: {csv_path}")


# -------------------------------------------------
# PLOT 1: HISTOGRAM / BAR CHART OF COUNTS BY BAND
# -------------------------------------------------

plt.figure(figsize=(12, 6))
plt.bar(df["canonical_label"], df["n_events"])
plt.xticks(rotation=45, ha="right")
plt.xlabel("Fire damage area band")
plt.ylabel("Incident count")
plt.title("FRIS fire damage area bands: incident counts")
plt.tight_layout()

plot1_path = OUTPUT_DIR / "fire_damage_band_histogram_counts.png"
plt.savefig(plot1_path, dpi=300)
plt.close()

print(f"Wrote: {plot1_path}")


# -------------------------------------------------
# PLOT 2: DENSITY + POWER-LAW FIT + q33 POSITIONS
# -------------------------------------------------

x_curve = np.logspace(
    np.log10(fit_df["lower_m2"].min() + 1e-6),
    np.log10(fit_df["upper_m2"].max()),
    400,
)
y_curve = power_law_density(x_curve, power_a, power_b)

plt.figure(figsize=(12, 7))

# Observed band densities at midpoints.
plt.scatter(
    fit_df["midpoint_m2"],
    fit_df["event_density_per_m2"],
    label="Observed density at band midpoint",
)

# Fitted power-law curve.
plt.plot(
    x_curve,
    y_curve,
    label=f"Power-law fit: density = {power_a:.2f} × area^(-{power_b:.2f})",
)

# q33 representative positions, plotted on the fitted curve.
q33_x = out["q33_m2"].to_numpy()
q33_y = power_law_density(q33_x, power_a, power_b)

plt.scatter(
    q33_x,
    q33_y,
    marker="x",
    s=80,
    label="q0.33 representative position",
)

for _, row in out.iterrows():
    plt.annotate(
        row["band_label"],
        (row["q33_m2"], power_law_density(np.array([row["q33_m2"]]), power_a, power_b)[0]),
        textcoords="offset points",
        xytext=(4, 4),
        fontsize=8,
    )

plt.xscale("log")
plt.yscale("log")
plt.xlabel("Area (m²)")
plt.ylabel("Incident density per m²")
plt.title("Fire damage area density and fitted power-law")
plt.legend()
plt.tight_layout()

plot2_path = OUTPUT_DIR / "fire_damage_band_density_powerlaw_q33.png"
plt.savefig(plot2_path, dpi=300)
plt.close()

print(f"Wrote: {plot2_path}")