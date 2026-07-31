from __future__ import annotations

"""
Area-band conversion and damage-fraction helpers for the Fire Emissions model.

The FRIS data records fire and total damage area as bands such as:
    - None
    - Up to 5
    - 6-10
    - 11-20
    - Over 10,000

Stage 1 needs numeric values.  This module converts each band into three
values: low, default and high.  The default value is deliberately lower-skewed,
because we expect larger damage areas to be less common within each band than
smaller damage areas.
"""

import re
from dataclasses import dataclass
from typing import Optional

from scripts.fire.fire_model_records import AreaEstimate


# -----------------------------------------------------------------------------
# CONFIGURATION
# -----------------------------------------------------------------------------

# Position of the default value inside a closed area band.
# Example: 11-20 m2 gives default = 11 + 0.33 * (20 - 11).
DEFAULT_WITHIN_BAND_POSITION = 0.33

# Text values that should be treated as missing rather than as a real area band.
MISSING_TEXT = {"", "null", "nan", "na", "n/a"}

# FRIS uses "None" as a valid zero-area band.  Do not treat this as missing.
NONE_BAND_TEXT = "none"


# -----------------------------------------------------------------------------
# SMALL DATA OBJECTS
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class FractionResult:
    """
    Result of converting an area to a fraction of a denominator.

    capped is True when the raw fraction was greater than the allowed maximum
    and has been clipped.
    """

    fraction: Optional[float]
    capped: bool = False


# -----------------------------------------------------------------------------
# AREA BAND PARSING
# -----------------------------------------------------------------------------

def clean_area_label(value: object) -> Optional[str]:
    """
    Convert an input area-band value into a stripped string.

    Returns None for missing values, but keeps "None" as a real FRIS category.
    """
    if value is None:
        return None

    text = str(value).strip()
    if text.lower() in MISSING_TEXT:
        return None

    return text


AREA_BAND_DEFAULT_POSITION = 0.33 # estimated half-counts position within each area band (power-law relationship)

def parse_area_band_estimate(value: object) -> dict[str, Optional[float]]:
    """
    Convert a FRIS area-band label to model-facing area estimates.

    FRIS area bands are treated as deterministic grouped inputs,
    not as uncertainty ranges. Therefore ordinary area calculations use one
    representative within-band value for all estimate cases.

    Current rule:
        For closed area bands, use q33:

            area_m2 = lower_bound + 0.33 * (upper_bound - lower_bound)

        For "None", use 0.

        The 33th percentile was taken from applying a power-law fit to the
        counts within each FRIS area band using `scripts/dev_area_band_fit.py`.

        For open-ended bands, use the current parser fallback from
        parse_area_band_position(); this should be revisited if open-ended
        bands become material in the model output.

    Important:
        This function should not be used for area-mismatch fallbacks.

        Fallbacks should call parse_area_band_position() directly with the
        required positions, for example:

            same-band fallback:
                room origin size = q25
                fire damage area = q75

            ordered-band fallback:
                room origin size = q75
                fire damage area = q25

    Returns
    -------
    dict[str, Optional[float]]
        A dictionary with keys low, default and high. All three values are the
        same ordinary q33 area estimate.
    """
    area_m2 = parse_area_band_position(
        value,
        position=AREA_BAND_DEFAULT_POSITION,
    )

    return {
        "low": area_m2,
        "default": area_m2,
        "high": area_m2,
    }

def parse_area_band_position(
    value: object,
    *,
    position: float,
) -> Optional[float]:
    """
    Parse one FRIS area-band label and return a value at a chosen within-band
    position.

    Parameters
    ----------
    value:
        FRIS area-band label, e.g. "Up to 5", "6-10", "101-200".

    position:
        Within-band position on [0, 1].

        Examples:
            0.25 = 25th percentile proxy
            0.75 = 75th percentile proxy

    Returns
    -------
    Optional[float]
        Area estimate in m2, or None if the label cannot be parsed.

    Notes
    -----
    This is mainly used for multiple_rooms fallbacks, where the event category
    says the fire spread beyond the origin room but coarse FRIS area bands do
    not produce a positive residual area using the ordinary central estimate.
    """
    if position < 0.0 or position > 1.0:
        raise ValueError("position must be between 0 and 1.")

    label = clean_area_label(value)

    if label is None:
        return None

    label_lower = label.strip().lower()

    # FRIS "None" is a real zero-area category.
    if label_lower == NONE_BAND_TEXT:
        return 0.0

    normalised = label_lower.replace(",", "")

    # Case: "Up to 5".
    match = re.search(r"up\s*to\s*(\d+(?:\.\d+)?)", normalised)
    if match:
        lower = 0.0
        upper = float(match.group(1))
        return lower + position * (upper - lower)

    # Case: "6-10", "6 – 10", "6 to 10".
    match = re.search(
        r"(\d+(?:\.\d+)?)\s*(?:-|–|—|to)\s*(\d+(?:\.\d+)?)",
        normalised,
    )
    if match:
        lower = float(match.group(1))
        upper = float(match.group(2))
        return lower + position * (upper - lower)

    # Case: "Over 10,000".
    # No defensible upper bound is available, so return the lower threshold.
    match = re.search(r"over\s*(\d+(?:\.\d+)?)", normalised)
    if match:
        return float(match.group(1))

    # Case: a plain numeric value, useful for future scenario rows.
    try:
        number = float(normalised)
        if number >= 0:
            return number
    except ValueError:
        pass

    return None


# -----------------------------------------------------------------------------
# FRACTION / CAPPING HELPERS
# -----------------------------------------------------------------------------

def clip_fraction(value: Optional[float], *, max_fraction: float = 1.0) -> FractionResult:
    """
    Clip a fraction to [0, max_fraction].
    """
    if value is None:
        return FractionResult(fraction=None, capped=False)

    if value < 0:
        return FractionResult(fraction=0.0, capped=True)

    if value > max_fraction:
        return FractionResult(fraction=max_fraction, capped=True)

    return FractionResult(fraction=value, capped=False)


def fraction_from_area(
    area_m2: Optional[float],
    denominator_m2: Optional[float],
    *,
    max_fraction: float = 1.0,
) -> FractionResult:
    """
    Convert an area to a capped fraction of a denominator.

    Returns None if either value is missing or if the denominator is zero.
    """
    if area_m2 is None:
        return FractionResult(fraction=None, capped=False)

    if denominator_m2 is None or denominator_m2 <= 0:
        return FractionResult(fraction=None, capped=False)

    return clip_fraction(area_m2 / denominator_m2, max_fraction=max_fraction)


def capped_area(area_m2: Optional[float], cap_m2: Optional[float]) -> tuple[Optional[float], bool]:
    """
    Cap an area by a physical maximum, such as room size or dwelling size.
    """
    if area_m2 is None:
        return None, False

    if cap_m2 is None or cap_m2 <= 0:
        return area_m2, False

    if area_m2 > cap_m2:
        return cap_m2, True

    return area_m2, False
