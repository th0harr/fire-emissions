# Ingestion Command Line Interface (CLI) Reference

All ingestion operations in **Fire Emissions** are routed through: `scripts/ingest.py`

This script resolves paths from `config/local_paths.yaml`, plans ingestion and pruning actions, and executes database modifications under a file lock.

---

## Command Structure
```
python scripts/ingest.py \
  --profile <name> \
  --db <db_handle> \
  --type <ingest_type> \
  (--scan | --file <path>) \
  [--prune] \
  [--apply]
```

---

## Required Arguments

#### `--profile <name>`

Profile defined in: `config/local_paths.yaml` under `profiles`

Example: `--profile tom`

The profile determines:

- SharePoint root location


#### `--db <db_handle>`

Database handle defined in: `config/local_paths.yaml` under `db_roots`

Example: `--db inventory_db`

Current valid values: `inventory_db`, `test_db`, `fire_db`

The database handle determines:

- Which database root folder is used
- Which SQLite database file is used
- which raw data types are permitted for that database

This also allows the CLI to prevent invalid combinations, such as attempting to ingest `survey` data into `fire_db`.


#### `--type <ingest_type>`

Specifies which **ingestion module** to use.

Example: `--type survey`

Current valid values: `survey`, `vocab`

These correspond to entries within the `INGESTERS` dictionary, inside `scripts/ingest.py`.

The selected type must also be allowed by the chosen `--db` entry in `config/local_paths.yaml` via its `raw_types` list.


## Required Mode (Choose One)

You must **select** exactly one of the following:

#### `--scan`

Scan the configured raw directory for that ingest type and plan ingestion of **all new files**.

Example: `--scan`


#### `--file <path>`

Ingest a **single file** directly.

Example: `--file "C:\path\to\mapping_list.xlsx"`

---

## Optional Flags

#### `--prune`


**Preview** deletion of database records that are no longer represented in the current ingest source.

The exact behaviour depends on the ingest type. For example:

- source files that are no longer present
- database rows that are no longer present in the current ingest file

This creates an easy method of removing obsolete objects from the database, without having to do so manually.
Simply remove the source/rows from the source file and re-ingest.

No changes occur unless combined with `--apply`.


#### `--apply`

Execute destructive operations:

- Insert new records
- Delete pruned records

Without `--apply`, the script runs in **dry-run mode**.



## Default Behaviour (Dry-Run Mode)

If `--apply` is not specified:

- Resolved paths are printed
- Raw files are scanned
- Ingestion plan is generated
- New files are listed
- Prune candidates are displayed (if **--prune**)
- No database modifications occur

---

## Database Safety

All write operations are executed under a database **file lock** to prevent simultaneous modification by multiple users.

Writes **only** occur when:

`--apply` is specified **AND** 

new files are detected **OR** `--prune` is specified

---

## Example Commands

### Survey Ingestion (Dry-Run Scan)
`python scripts/ingest.py --profile tom --db inventory_db --type survey --scan`

### Survey Ingestion (Apply)
`python scripts/ingest.py --profile tom --db inventory_db --type survey --scan --apply`

### Vocab Ingestion (Single File, Dry-Run)
`python scripts/ingest.py --profile tom --type vocab --file "C:\path\to\mapping_list.xlsx"`

### Vocab Ingestion (Apply)
`python scripts/ingest.py --profile tom --db inventory_db --type vocab --file "C:\path\to\mapping_list.xlsx" --apply`

### Survey Prune Preview
`python scripts/ingest.py --profile tom --db inventory_db --type survey --scan --prune`

### Survey Prune + Apply
`python scripts/ingest.py --profile tom --db inventory_db --type survey --scan --prune --apply`
