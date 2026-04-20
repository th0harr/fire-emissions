# Modelling System Architecture

This document describes the internal modelling framework used by
`scripts/model.py`.

The modelling system is modular. Each model type (e.g. `inventory`,
and future types such as `room_carbon`, `fire`, or `lca`) is implemented
as a separate module and registered in the `MODELLERS` dictionary inside
`scripts/model.py`.

---

## Path Resolution

`scripts/model.py` resolves paths from `config/local_paths.yaml` using two inputs:

- `--profile` selects the local SharePoint root
- `--db` selects the database root and SQLite database file from `db_roots`

Unlike `scripts/ingest.py`, the modelling dispatcher does **not** currently
resolve a raw-data path, because modelling actions operate on data that have
already been ingested into the SQLite database.

The modelling type is selected using:

- `--type` selects the modelling action from the `MODELLERS` dictionary

---

## Internal Model Module Interface

Each model type module must implement the following function:

1. `build_<x>(db_path)`

For example, the current inventory model module exposes:

- `build_inventory_distributions(db_path)`

This function is called by `scripts/model.py` and must follow the expected
behaviour described below.

---

### `build_<x>(db_path)`

Performs the requested modelling action using the resolved SQLite database.

This function should:

- validate that required source and target tables exist
- check that required source data are present
- acquire a database file lock before any destructive write operation
- perform the modelling calculation
- write derived results back to the database
- return a summary dictionary describing the work completed

This function is responsible for the full modelling build for that type.

---

## Current Model Type: `inventory`

The current registered model type is:

- `inventory`

This corresponds to the module:

- `scripts/build_inventory_distributions.py`

and is called via:

```bash
python -m scripts.model --profile <profile> --db <db_handle> --type inventory
```

---

## Current Inventory Modelling Scope

The current `inventory` model type rebuilds the following intermediate tables:

- `item_count_pmf`
- `item_count_summary`
- `room_count_pmf`
- `room_count_summary`

These are built from the following source tables:

- `inventory_observations`
- `dwelling_observations`

### Current design choices

The current implementation uses:

- raw empirical frequencies only
- zero counts included in the observed distributions
- fixed support `0..10` inclusive for both item and room counts
- delete → rebuild workflow for target modelling tables
- interpolated quartiles (`count_q25`, `count_q75`) as spread descriptors
- expected count calculated from the PMF representation

The resulting tables are intended as **intermediate modelling tables** for use
by later downstream calculations, such as room-level carbon stock and fire/emissions
estimation.

---

## Database Safety

All destructive modelling writes are executed under a database **file lock**
to prevent simultaneous modification by multiple users.

For the current `inventory` model type, the following tables are cleared and
rebuilt each time the modelling action is run:

- `item_count_pmf`
- `item_count_summary`
- `room_count_pmf`
- `room_count_summary`

This is intentional: these are intermediate summary tables derived from the
current ingested survey data, so full rebuilds are simpler and easier to
reason about than incremental updates.

---

## Returned Summary

Each model module should return a summary dictionary describing the work performed.

For `inventory`, this currently includes:

- item groups processed
- item PMF rows written
- item summary rows written
- room groups processed
- room PMF rows written
- room summary rows written

This summary is printed by `scripts/model.py` after the modelling action completes.

---

## Adding a New Model Type

To add a new model type:

1. Create a new module in `scripts/` implementing the required build function.
2. Import the module in `scripts/model.py`.
3. Add it to the `MODELLERS` dictionary.
4. Ensure the required database schema/tables exist in the relevant database.
5. Document the new model type and any required source tables.

Once registered, the new type becomes available via:

```bash
python -m scripts.model --profile <profile> --db <db_handle> --type <type>
```

---

## Example: Inventory Model Build

```bash
python -m scripts.model --profile tom --db test_db --type inventory
```

This resolves the SQLite database path from `config/local_paths.yaml`,
validates the required source/target tables, and rebuilds the inventory-derived
count PMF and count summary tables.