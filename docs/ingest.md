# Ingestion System Architecture

This document describes the internal ingestion framework used by
`scripts/ingest.py`.

The ingestion system is modular. Each ingest type (e.g. survey, showroom, vocab)
is implemented as a separate module and registered in the `INGESTERS`
dictionary inside `scripts/ingest.py`.

---

## Internal Ingest Module Interface

Each ingest type module must implement the following functions:

1. `scan_inputs(raw_dir)`
2. `plan(db_path, raw_dir, input_files)`
3. `prune_preview(db_path, raw_dir)`
4. `prune_apply(db_path, raw_dir)`
5. `ingest_apply(db_path, raw_dir, new_files)`

These functions are called by `scripts/ingest.py` and must follow the expected behaviour described below.

---

### `scan_inputs(raw_dir)`

Returns a list of input files found in the configured raw directory.

This function should:
- Inspect the directory for valid source files.
- Return a list of `Path` objects.
- Raise an error if the structure is invalid (optional but recommended).

---

### `plan(db_path, raw_dir, input_files)`

Generates a read-only ingestion plan.

Returns a dictionary containing:

- `"new"` → list of new files to ingest
- `"already_ingested"` → count of existing sources (optional)

This function must not modify the database.

---

### `prune_preview(db_path, raw_dir)`

Identifies database entries whose raw source file no longer exists.

Returns a list of prune candidates.

Must not modify the database.

---

### `prune_apply(db_path, raw_dir)`

Executes deletion of prune candidates.

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
4. Add the corresponding path to `config/local_paths.yaml`.

Once registered, the new type becomes available via:

```bash
python scripts/ingest.py --type <new_type> ...
