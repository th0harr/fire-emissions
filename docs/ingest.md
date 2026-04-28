# Ingestion System Architecture

This document describes the internal ingestion framework used by
`scripts/ingest.py`.

The ingestion system is modular. Each ingest type (e.g. `survey`, `vocab`, `assumed`, and future types such as `fris`) is implemented as a separate module and  registered in the `INGESTERS` dictionary inside  scripts/ingest.py`.

---

## Path Resolution

`scripts/ingest.py` resolves paths from `config/local_paths.yaml` using three inputs:

- `--profile` selects the local SharePoint root
- `--db` selects the database root and SQLite database file from `db_roots`
- `--type` selects the raw-data path from `paths`

The selected ingest type must also be permitted by the chosen database handle
via its `raw_types` list.

---

## Internal Ingest Module Interface

Each ingest type module must implement the following functions:

1. `scan_inputs(raw_dir)`
2. `plan(db_path, raw_dir, input_files)`
3. `prune_preview(db_path, raw_dir)`
4. `prune_apply(db_path, raw_dir)`
5. `ingest_apply(db_path, raw_dir, new_files)`

These functions are called by `scripts/ingest.py` and must follow the expected
behaviour described below.

---

### `scan_inputs(raw_dir)`

Returns a list of input files found in the configured raw directory.

This function should:
- inspect the directory for valid source files
- return a list of `Path` objects
- raise an error if the structure is invalid, if appropriate

---

### `plan(db_path, raw_dir, input_files)`

Generates a read-only ingestion plan.

Returns a dictionary containing:

- `"new"` → list of new files to ingest
- `"already_ingested"` → count of existing sources, if relevant

This function must not modify the database.

---

### `prune_preview(db_path, raw_dir)`

Identifies database entries that are no longer represented in the current ingest source.

The exact behaviour depends on the ingest type. For example:

- for file-based ingest types, this may mean source files that no longer exist
- for canonical single-file ingest types, this may mean rows or records no longer present in the current ingest file

Returns a list or structured summary of prune candidates.

Must not modify the database.

Note that separate prune logic is not currently implemented for `vocab`, or `assumed` because these ingests use `replace_all` by default and therefore removes obsolete rows during `ingest_apply()`.

---

### `prune_apply(db_path, raw_dir)`

Executes deletion of prune candidates identified by `prune_preview()`.

Returns a summary dictionary describing the deletions performed.

---

### `ingest_apply(db_path, raw_dir, new_files)`

Performs the actual ingestion of new files.

This function is executed under a database file lock.

Returns a summary dictionary describing:
- rows inserted
- rows deleted
- sources added
- or other relevant statistics

---

## Adding a New Ingest Type

To add a new ingest type:

1. Create a new module in `scripts/` implementing the required interface.
2. Import the module in `scripts/ingest.py`.
3. Add it to the `INGESTERS` dictionary.
4. Add the corresponding raw-data path to `paths` in `config/local_paths.yaml`.
5. Add the new ingest type to the `raw_types` list of each database handle that should allow it.

Once registered, the new type becomes available via:

```
python -m scripts.ingest --profile <profile> --db <db_handle> --type <type>
```