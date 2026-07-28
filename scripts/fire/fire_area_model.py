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


def parse_area_band_estimate(
    value: object,
    *,
    default_position: float = DEFAULT_WITHIN_BAND_POSITION,
) -> AreaEstimate:
    """
    Parse one FRIS area-band label into low/default/high m2 values.

    Notes
    -----
    This function deliberately avoids relying on hidden hard-coded lists of
    bands.  It uses the label text itself, so it will still work if the mapping
    workbook is edited as long as the labels remain in the same general style.
    """
    label = clean_area_label(value)

    if label is None:
        return AreaEstimate(
            input_label=None,
            low_m2=None,
            default_m2=None,
            high_m2=None,
        )

    label_lower = label.strip().lower()

    # FRIS "None" means a real zero-area category.
    if label_lower == NONE_BAND_TEXT:
        return AreaEstimate(
            input_label=label,
            low_m2=0.0,
            default_m2=0.0,
            high_m2=0.0,
            is_none_band=True,
        )

    # Remove commas so "1,001" becomes "1001" for numeric parsing.
    normalised = label_lower.replace(",", "")

    # Case: "Up to 5".
    match = re.search(r"up\s*to\s*(\d+(?:\.\d+)?)", normalised)
    if match:
        upper = float(match.group(1))
        lower = 0.0
        default = lower + default_position * (upper - lower)
        return AreaEstimate(
            input_label=label,
            low_m2=lower,
            default_m2=default,
            high_m2=upper,
        )

    # Case: "6-10", "6 – 10", "6 to 10".
    match = re.search(
        r"(\d+(?:\.\d+)?)\s*(?:-|–|—|to)\s*(\d+(?:\.\d+)?)",
        normalised,
    )
    if match:
        lower = float(match.group(1))
        upper = float(match.group(2))
        default = lower + default_position * (upper - lower)
        return AreaEstimate(
            input_label=label,
            low_m2=lower,
            default_m2=default,
            high_m2=upper,
        )

    # Case: "Over 10,000".
    # For the open-ended band, we avoid inventing an arbitrary upper value here.
    # The actual model will cap the area by room/dwelling size where needed.
    match = re.search(r"over\s*(\d+(?:\.\d+)?)", normalised)
    if match:
        lower = float(match.group(1))
        return AreaEstimate(
            input_label=label,
            low_m2=lower,
            default_m2=lower,
            high_m2=lower,
            is_open_ended=True,
        )

    # Case: a plain numeric value, useful for future scenario rows.
    try:
        number = float(normalised)
        if number >= 0:
            return AreaEstimate(
                input_label=label,
                low_m2=number,
                default_m2=number,
                high_m2=number,
            )
    except ValueError:
        pass

    # If we cannot parse the band, return an empty estimate.  The stock model
    # will decide whether this is a blocking problem for the event/category.
    return AreaEstimate(
        input_label=label,
        low_m2=None,
        default_m2=None,
        high_m2=None,
    )


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
