from __future__ import annotations

"""
Molecular conversion factors for Fire Emissions Stage 2.

Stage 2 works in two related mass units:

1. kgC
   Kilograms of carbon atoms.

2. kg species
   Kilograms of the full emitted molecule, for example CO2 or CO.

Why conversion is needed
------------------------
The Stage 1 model estimates affected carbon stock in kgC.

When we convert this to a combustion species such as CO2, the emitted molecule
contains carbon plus oxygen.  Therefore, 1 kg of carbon emitted as CO2 becomes
more than 1 kg of CO2 gas.

Molecular weights:
    C   = 12
    O   = 16
    CO2 = 12 + 16 + 16 = 44
    CO  = 12 + 16      = 28

Therefore:
    kg CO2 = kgC * 44 / 12
    kg CO  = kgC * 28 / 12

These constants are kept in their own small module so that the Stage 2 species
model is easier to read.
"""


# -----------------------------------------------------------------------------
# MOLECULAR MASS RATIOS
# -----------------------------------------------------------------------------

# Convert kg of carbon atoms into kg of carbon dioxide molecule.
C_TO_CO2 = 44.0 / 12.0

# Convert kg of carbon atoms into kg of carbon monoxide molecule.
C_TO_CO = 28.0 / 12.0


# -----------------------------------------------------------------------------
# PUBLIC HELPER
# -----------------------------------------------------------------------------

def carbon_to_species_mass_factor(species: str) -> float:
    """
    Return the molecular conversion factor for one emitted species.

    Parameters
    ----------
    species:
        Species name such as "CO2" or "CO".  The comparison is case-insensitive.

    Returns
    -------
    float
        Multiplier used to convert kgC into kg emitted molecule.

    Raises
    ------
    ValueError
        If the species is not yet supported.

    Notes
    -----
    This first implementation supports only CO2 and CO because those are the
    current planned Stage 2 species.  Later, species such as CH4 could be added
    here if the emission-parameter workbook is extended.
    """
    species_clean = str(species).strip().upper()

    if species_clean == "CO2":
        return C_TO_CO2

    if species_clean == "CO":
        return C_TO_CO

    raise ValueError(
        f"Unsupported emitted species: {species!r}. "
        "Currently supported species are: CO2, CO."
    )