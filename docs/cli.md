# Ingestion Command Line Interface (CLI) Reference

All ingestion operations in **Fire Emissions** are routed through: `scripts/ingest.py`

This script resolves paths from `config/local_paths.yaml`, plans ingestion and pruning actions, and executes database modifications under a file lock.

---

## Command Structure
```
python scripts/ingest.py \
  --profile <name> \
  --type <ingest_type> \
  (--scan | --file <path>) \
  [--prune] \
  [--apply]
```

---

## Required Arguments

#### `--profile <name>`

Profile defined in: `config/local_paths.yaml`

Example: `--profile tom`

The profile determines:

- SharePoint root location
- Database file path
- Raw data directories

#### `--type <ingest_type>`

Specifies which **ingestion module** to use.

Current valid values: `showroom, survey, vocab`

These correspond to entries within the `INGESTERS` dictionary, inside `scripts/ingest.py`.


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

**Preview** deletion of database records whose associated raw source file is **missing**.

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

`--apply` is specified **AND** new files are detected, 

**OR**

`--prune` is specified

---

## Example Commands

### Survey Ingestion (Dry-Run Scan)
`python scripts/ingest.py --profile tom --type survey --scan`

### Survey Ingestion (Apply)
`python scripts/ingest.py --profile tom --type survey --scan --apply`

### Vocab Ingestion (Single File, Dry-Run)
`python scripts/ingest.py --profile tom --type vocab --file "C:\path\to\mapping_list.xlsx"`

### Vocab Ingestion (Apply)
`python scripts/ingest.py --profile tom --type vocab --file "C:\path\to\mapping_list.xlsx" --apply`

### Showroom Prune Preview
`python scripts/ingest.py --profile tom --type showroom --scan --prune`

### Showroom Prune + Apply
`python scripts/ingest.py --profile tom --type showroom --scan --prune --apply`