# Command Line Interface (CLI) Reference

The Fire Emissions project currently uses three main command-line entry points:

1. `scripts.ingest` — generic ingestion dispatcher for raw/source data.
2. `scripts.lca.fetch_amazon_prices` — specialised LCA helper command for Amazon UK price scraping and provisional embodied CO2 calculation.
3. `scripts.model` — generic modelling dispatcher for derived inventory/carbon tables.

The ingestion and modelling dispatchers route requests to domain-specific modules under `scripts/inventory/`, while the LCA helper command reads from the database and writes provisional item-level embodied-carbon estimates to `embodied_carbon_data`.

All commands resolve database paths from `config/local_paths.yaml`.

---

# 1) Ingestion CLI

All ingestion operations in **Fire Emissions** are routed through: `python -m scripts.ingest`

This script resolves paths from `config/local_paths.yaml`, plans ingestion and pruning actions, and executes database modifications under a file lock.

## Command Structure
```bash
python -m scripts.ingest \
  --profile <name> \
  --db <db_handle> \
  --type <ingest_type> \
  (--scan | --file <path>) \
  [--prune] \
  [--apply]
```

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

Current valid values include:

- `survey`
- `vocab`
- `assumed`
- `fire_event`
- `emissions`

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

Note that this operation is redundant for `--type vocab` and `--type assumed`, because both ingestion types use a full rewrite by default when `--apply` is used. As a result, separate prune logic is not currently implemented for `vocab` or `assumed`.

#### `--apply`

Execute destructive operations:

- Insert new records
- Delete pruned records

Without `--apply`, the script runs in **dry-run mode**.


#### `--overwrite`

Only applies to this specific fire-event build helper command. It is not a general ingest option.

Performs a full refresh of the `fire_events` table prior to adding a new row (omitting this appends a new row to the existing data).


## Default Behaviour (Dry-Run Mode)

If `--apply` is not specified:

- Resolved paths are printed
- Raw files are scanned
- Ingestion plan is generated
- New files are listed
- Prune candidates are displayed (if `--prune`)
- No database modifications occur


If `--overwrite` is not specified during `scripts.fire.build_fire_event_input`,
then a new row is appended to the existing data by default (non-destructive).


## Database Safety

All write operations are executed under a database **file lock** to prevent simultaneous modification by multiple users.

Writes **only** occur when:

`--apply` is specified **AND**

new files are detected **OR** `--prune` is specified

## Example Commands

### Survey Ingestion (Dry-Run Scan)
```bash
python -m scripts.ingest --profile tom --db inventory_db --type survey --scan
```

### Survey Ingestion (Apply)
```bash
python -m scripts.ingest --profile tom --db inventory_db --type survey --scan --apply
```

### Vocab Ingestion (Single File, Dry-Run)
```bash
python -m scripts.ingest --profile tom --db inventory_db --type vocab --file "C:\path\to\mapping_list.xlsx"
```

### Vocab Ingestion (Apply)
```bash
python -m scripts.ingest --profile tom --db inventory_db --type vocab --file "C:\path\to\mapping_list.xlsx" --apply
```

### Assumed Inventory Ingestion (Dry-Run Scan)
```bash
python -m scripts.ingest --profile tom --db inventory_db --type assumed --scan
```

### Assumed Inventory Ingestion (Apply)
```bash
python -m scripts.ingest --profile tom --db inventory_db --type assumed --scan --apply
```

### Survey Prune Preview
```bash
python -m scripts.ingest --profile tom --db inventory_db --type survey --scan --prune
```

### Survey Prune + Apply
```bash
python -m scripts.ingest --profile tom --db inventory_db --type survey --scan --prune --apply
```

### Fire Event Input Ingestion (Dry-Run Scan)
```bash
python -m scripts.ingest --profile tom --db fire_db --type fire_event --scan
```

### Fire Event Input Ingestion (Apply)
```bash
python -m scripts.ingest --profile tom --db fire_db --type fire_event --scan --apply
```

### Build Model-Facing Fire Event Record
```bash
python -m scripts.fire.build_fire_event_input --profile tom --db fire_db --apply
```

### Fire Emission Parameter Ingestion (Dry-Run Scan)

```bash
python -m scripts.ingest --profile tom --db fire_db --type emissions --scan
```

### Fire Emission Parameter Ingestion (Apply)

```bash
python -m scripts.ingest --profile tom --db fire_db --type emissions --scan --apply
```


---



# 2) LCA / Embodied Carbon Price Scraper


This script retrieves Amazon UK price samples for canonical inventory items and writes provisional embodied-carbon values to the `embodied_carbon_data` table.

Unlike `scripts.ingest`, this is not a raw-data ingestion controller.
It is a database-backed LCA helper script that:

- Scrapes example prices from products matching input search terms
- Calculates the spend-based embodied emissions from these prices

## Command Structure
```bash
python -m scripts.lca.fetch_amazon_prices \
  --profile <name> \
  --db <db_handle> \
  [--limit <n>] \
  [--only-missing] \
  [--sleep <seconds>] \
  [--max-prices <n>] \
  [--timeout <seconds>] \
  [--user-agent <string>] \
  [--dry-run]
```

## Required Arguments

#### `--profile`

Profile defined in `config/local_paths.yaml` under `profiles`.

Example: `--profile tom`


#### `--db`

Database handle defined in `config/local_paths.yaml` under `db_roots`.

Example: `--db inventory_db`

For testing, use `test_db first`. Once checked, run against `inventory_db`.


## Optional Arguments

#### `--limit <n>`

Process only the first `n` items from `item_dictionary`.
Useful for testing, as it is much quicker than scraping every item.

Example: `--limit 5`


#### `--only-missing`

Only process items that do not already have an amazon_price_mean in embodied_carbon_data.
Useful when a few new rows are added or a previous run was interrupted.

Example: `--only-missing`


#### `--sleep <seconds>`

Seconds to wait between Amazon requests.

Example: `--sleep 5`

Default: `--sleep 2`


#### `--max-prices <n>`

Maximum number of Amazon prices to store per item.

Must be between 1 and 10, because the database stores 10 fixed columns.

Example: `--max-prices 5`

Default: `--max-prices 10`


#### `--timeout <seconds>`

Request timeout for each Amazon search.

Example: `--timeout 20`

Default: `--timeout 30`


#### `--user-agent <string>`

Override the browser user-agent string sent to Amazon.
Most users should not need this unless Amazon blocks or changes responses.


#### `--dry-run`

Run the workflow without writing to the database.

The script will still:

- resolve paths
- read items from the database
- fetch Amazon prices
- calculate replacement cost
- calculate embodied CO2
- print results

but it will not insert or update rows in `embodied_carbon_data`.


## Example commands

#### Amazon Price Scraper Test Run

Dry-run the first 3 items without writing to the database:

```bash
python -m scripts.lca.fetch_amazon_prices --profile tom --db test_db --limit 3 --dry-run
```

#### Amazon Price Scraper Small Write Test

Process the first 3 items and write/update `embodied_carbon_data`:

```bash
python -m scripts.lca.fetch_amazon_prices --profile tom --db test_db --limit 3
```

#### Amazon Price Scraper Full Test Database Run

Process all items in test_db:

```bash
python -m scripts.lca.fetch_amazon_prices --profile tom --db test_db
```

#### Amazon Price Scraper Production Run

After testing, run against the main inventory database:

```bash
python -m scripts.lca.fetch_amazon_prices --profile tom --db inventory_db
```


#### Amazon Price Scraper Only Missing Rows

Process only items that do not yet have scraped Amazon price values:

```bash
python -m scripts.lca.fetch_amazon_prices --profile tom --db inventory_db --only-missing
```

## Recommended Workflow Order

1. Clean rebuild of database
2. **Amazon price scraper**
3. Build model tables


## Output table

The Amazon price scraper writes to: `embodied_carbon_data`.

The table stores one current row per item_name. Re-running the scraper updates existing rows rather than creating duplicates.

The main calculated fields are:

```bash
amazon_price_mean
amazon_price_std
amazon_price_upper
replacement_cost_adjusted
embodied_CO2_kg
```

The scraper deliberately uses Amazon a-price-whole values only.
This means pence are not retained, but it avoids occasional parsing artefacts where a price such as 37.50 may be misread as 3750.


Typical successful output is of the form:
```text
Resolved paths:
  DB HANDLE: test_db
  DB:        C:\...\pooled_inventory.sqlite

Items to process: ...
Dry run:          ...

...
...
...


Amazon price / embodied CO2 workflow complete:
  Rows attempted:             ...
  Rows written:               ...
  Amazon fetch failures:      ...
  Rows without price basis:   ...
  Rows without embodied CO2:  ...

```

---


# 3) Modelling CLI

All modelling operations in **Fire Emissions** are routed through: `scripts/model.py`

This script resolves the SQLite database path from `config/local_paths.yaml`, selects the requested modelling action, and executes the modelling build under a database file lock where required.

The modelling CLI is intended for building **derived / intermediate tables**, rather than ingesting raw source data.

## Command Structure
```bash
python -m scripts.model \
  --profile <name> \
  --db <db_handle> \
  --type <model_type>
```

## Required Arguments

#### `--profile <name>`

Profile defined in: `config/local_paths.yaml` under `profiles`

Example: `--profile tom`

The profile determines:

- SharePoint root location

#### `--db <db_handle>`

Database handle defined in: `config/local_paths.yaml` under `db_roots`

Example: `--db test_db`

Current valid values depend on the configured modelling workflow, but typically include:

- `inventory_db`
- `test_db`

The database handle determines:

- Which database root folder is used
- Which SQLite database file is targeted for the modelling action

#### `--type <model_type>`

Specifies which **modelling action** to run.

Example: `--type inventory`

Current valid values:

- `inventory
- `room_carbon`

These correspond to entries within the `MODELLERS` dictionary, inside `scripts/model.py`.

#### `--assumed <include|exclude>`

Optional argument used by `--type room_carbon` to control assumed items.

Options:

- `include`: add assumed_inventory rows into room carbon stock calculation
- `exclude`: build room carbon stock from survey-derived inventory only

Default: `--assumed include`


### Current modelling type: `inventory`

`--type inventory` rebuilds the following survey-derived intermediate tables:

- `item_count_pmf`
- `item_count_summary`
- `room_count_pmf`
- `room_count_summary`

These are built from:

- `inventory_observations`
- `dwelling_observations`


### Current modelling type: `room_carbon`

`--type room_carbon` rebuilds the following room-level carbon stock table:

- `room_carbon_stock`

This is built from:

- `item_count_summary`
- `item_dictionary`
- `furniture`
- `assumed_inventory` when `--assumed include` is used

The `--assumed` option controls whether assumed inventory rows are included:

- `--assumed include` includes assumed inventory contributions
- `--assumed exclude` excludes assumed inventory contributions

If omitted, the default is:

`--assumed include`

Running the command will:

1. resolve the database path
2. validate required source and target tables
3. check that source data are present
4. delete existing rows from `room_carbon_stock`
5. rebuild `room_carbon_stock` from the current database contents

For `--assumed` include, the model also requires `assumed_inventory` to be populated. If this table is empty, run:

```bash
python -m scripts.ingest --profile tom --db test_db --type assumed --scan --apply
```


## Current Behaviour

Unlike `ingest.py`, the modelling CLI does **not** currently have:

- `--scan`
- `--file`
- `--prune`
- `--apply`

This is because the current modelling step is an explicit rebuild operation, not a dry-run planning workflow.

Running the command will:

1. resolve the database path
2. validate required source and target tables
3. check that source data are present
4. delete existing rows from the target modelling tables
5. rebuild the target modelling tables from the current database contents

## Database Safety

Destructive modelling operations are executed under a database **file lock** to prevent simultaneous modification by multiple users.

For the current `inventory` model type, the following tables are cleared and rebuilt each time:

- `item_count_pmf`
- `item_count_summary`
- `room_count_pmf`
- `room_count_summary`

## Example Commands

### Inventory Distribution Build
```bash
python -m scripts.model --profile tom --db test_db --type inventory
```

This rebuilds the survey-derived count PMF and count summary tables in the selected SQLite database.

Typical successful output is of the form:

```text
Resolved paths:
  DB HANDLE: test_db
  TYPE:      inventory
  DB:        C:\...\pooled_inventory.sqlite

Validating required tables...
Checking source data are present...
Clearing existing distribution tables...
Rebuilding item count distributions...
Rebuilding room count distributions...

Model applied successfully:
  Item groups processed:     ...
  Item PMF rows written:     ...
  Item summary rows written: ...
  Room groups processed:     ...
  Room PMF rows written:     ...
  Room summary rows written: ...
```

### Room Carbon Stock Build
```bash
python -m scripts.model --profile tom --db test_db --type room_carbon
```

This rebuilds `room_carbon_stock`, including assumed inventory by default.

Equivalent explicit command:

```bash
python -m scripts.model --profile tom --db test_db --type room_carbon --assumed include
```

#### Room Carbon Stock Build Without Assumed Inventory
```bash
python -m scripts.model --profile tom --db test_db --type room_carbon --assumed exclude
```

This rebuilds `room_carbon_stock` using survey-derived inventory only.

Typical successful output is of the form:

```text
Resolved paths:
  DB HANDLE: test_db
  TYPE:      room_carbon
  DB:        C:\...\pooled_inventory.sqlite

Validating required tables...
Checking source data are present...
Clearing existing room carbon stock table...
Rebuilding room carbon stock...

Model applied successfully:
  Source rows read:          ...
  Contributing item rows:    ...
  Room summary rows written: ...
  Assumed inventory:         include
```