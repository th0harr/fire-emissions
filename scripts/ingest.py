from __future__ import annotations

import argparse  # command-line argument parsing
from dataclasses import dataclass  # simple classes
from pathlib import Path  # safe file path handling on Windows
import yaml  # pyyaml

# Uses db_lock.py for file locking (prevent simultaneous write)
from scripts.db_lock import acquire_lock, release_lock, DatabaseLockedError

# Import source_type ingester modules (add more later)
from scripts import ingest_survey_export
from scripts import ingest_vocab
from scripts import ingest_assumed_items

INGESTERS = {
    "survey": ingest_survey_export,
    "vocab": ingest_vocab,
    "assumed": ingest_assumed_items,
}


# Container for current file paths
@dataclass(frozen=True)
class ResolvedPaths:
    """Paths resolved from profile + db_handle + config (using local_paths.yaml)."""
    db_path: Path  # root directory
    db_handle: str # database type
    raw_dir: Path  # ingest specific filepath

# Public function
def load_local_paths_config(config_path: Path) -> dict:
    """
    Load YAML config (local paths).
    Checks config/local_paths.yaml exists
    Parses YAML into a Python dictionary
    """
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config not found: {config_path}\n\n"
            f"Create it by copying:\n"
            f"  config/local_paths.example.yaml -> config/local_paths.yaml\n"
            f"and editing your profile's sharepoint_root."
        )

    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Config file is not a mapping/dict: {config_path}")
    return data


# Public function
def resolve_paths(profile: str, db_handle: str, ingest_type: str, config: dict) -> ResolvedPaths:
    """
    Resolve full local paths for:
      - the selected database
      - the raw directory for the chosen source type

    Expected config shape (example):
      profiles:
        tom:
          sharepoint_root: "C:/Users/.../Fire-Emissions-Databases"

      db_roots:
        inventory_db:
          root: "inventory_db"
          rel_db: "database/pooled_inventory.sqlite"
          raw_types: ["vocab", "showroom", "survey", "insurance"]

      paths:
        survey:
          rel_raw: "raw/surveys"
    """
    profiles = config.get("profiles", {})   # returns the selected profile
    db_roots = config.get("db_roots", {})   # returns the selected root directory
    paths    = config.get("paths", {})      # returns the relevant path

    # Check the profile is in local_paths.yaml (under profiles)
    if profile not in profiles:
        raise KeyError(
            f"Profile '{profile}' not found in config.\n"
            f"Available profiles: {', '.join(sorted(profiles.keys())) or '(none)'}"
        )

    # Check the database is in local_paths.yaml (under db_roots)
    if db_handle not in db_roots:
        raise KeyError(
            f"DB handle '{db_handle}' not found in config.\n"
            f"Available db handles: {', '.join(sorted(db_roots.keys())) or '(none)'}"
        )

    # Build the filepath to the DB root directory (via OneDrive sync)
    sharepoint_root = Path(profiles[profile]["sharepoint_root"])
    db_cfg = db_roots[db_handle]
    root = db_cfg.get("root")
    if not root:
        raise KeyError(f"Missing required db_roots.{db_handle}.root in config.")

    # Specific DB filepath
    rel_db = db_cfg.get("rel_db")
    if not rel_db:
        raise KeyError(f"Missing required db_roots.{db_handle}.rel_db in config.")

    # Validate permissible ingester type
    if ingest_type not in paths:
        raise KeyError(
            f"Path type '{ingest_type}' not found in config.paths.\n"
            f"Available path types: {', '.join(sorted(paths.keys())) or '(none)'}"
        )

    # Validate raw data is permissible for ingester
    raw_types = db_cfg.get("raw_types", [])
    if raw_types and ingest_type not in raw_types:
        raise ValueError(
            f"Ingest type '{ingest_type}' is not allowed for db '{db_handle}'.\n"
            f"Allowed raw types: {', '.join(raw_types)}"
        )
    
    # Get raw data filepath
    rel_raw = paths[ingest_type].get("rel_raw")
    if not rel_raw:
        raise KeyError(
            f"Missing required paths.{ingest_type}.rel_raw in config."
        )

    # Create full ingest paths for database and source
    db_path = sharepoint_root / Path(root) / Path(rel_db)
    raw_dir = sharepoint_root / Path(root) / Path(rel_raw)

    return ResolvedPaths(
        db_handle=db_handle,
        db_path=db_path,
        raw_dir=raw_dir,
    )


def main(argv: list[str] | None = None) -> int:
    """
    Command-line entry point for the ingestion dispatcher.
    Resolves paths, plans ingestion/pruning, and applies changes under DB lock.
    """
    parser = argparse.ArgumentParser(
        prog="ingest",
        description="Ingest raw data into the shared inventory SQLite database.",
    )

    parser.add_argument(
        "--profile",
        required=True,
        help="Profile name from config/local_paths.yaml (e.g. tom, sarka).",
    )

    parser.add_argument(
        "--db",
        required=True,
        help="Database handle from config/local_paths.yaml (e.g. inventory_db, test_db, fire_db).",
    )

    parser.add_argument(
        "--type",
        required=True,
        choices=sorted(INGESTERS.keys()),
        help="Raw data type to ingest (e.g. vocab, survey).",
    )

    # Two ingest methods: (i) all new files; OR (ii) a single specified file
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--scan", action="store_true", help="Scan the raw directory and ingest all new files.")
    mode.add_argument("--file", type=str, help="Ingest a single file (full path).")

    # Will prune all items from a deleted source (easy method to correct invalid ingests)
    parser.add_argument(
        "--prune",
        action="store_true",
        help="Find DB sources whose raw file is missing from the raw folder (to execulte add --apply).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply pruning actions (used with --prune and/or ingestion).",
    )

    # Build ingest routine
    args = parser.parse_args(argv)

    ingester = INGESTERS[args.type]

    # Load config + resolve paths (from local_paths.yaml)
    config = load_local_paths_config(Path("config") / "local_paths.yaml")
    resolved = resolve_paths(args.profile, args.db, args.type, config)

    print("Resolved paths:")
    print(f"  DB HANDLE: {args.db}")
    print(f"  TYPE:      {args.type}")
    print(f"  DB:        {resolved.db_path}")
    print(f"  RAW:       {resolved.raw_dir}")

    if not resolved.db_path.exists():
        print("\nERROR: Database file does not exist at resolved path.")
        print("Create it first by running init_db.py against the SharePoint-synced DB path.")
        return 2  # Error code: incorrect usage / invalid invocation

    # Determine input files via ingester (type-specific)
    if args.file:  # Returns a single file path
        input_files = [Path(args.file)]
    else:
        # Ingester.scan_inputs returns a list[Path]
        input_files = ingester.scan_inputs(resolved.raw_dir)

    # Provide feedback on ingest method used
    print(f"\nMode: {'single file' if args.file else 'scan'}")
    print(f"Found {len(input_files)} input file(s).")

    # Plan actions (read-only; no lock)
    # Compares raw files to DB to decide which are new (not already in DB)
    # ingester.plan returns a dict-like plan with keys:
    #   - new: list[Path]
    #   - already_ingested: int
    plan = ingester.plan(resolved.db_path, resolved.raw_dir, input_files)

    new_files = plan.get("new", [])     # Build list of new files
    already = plan.get("already_ingested", None)    # Produce count of already ingested

    if already is not None:
        print(f"\nAlready ingested sources ({args.type}): {already}")
        print(f"New files to ingest ({args.type}): {len(new_files)}")

    # Prints first 20 files for ingest
    # (Maybe modify if 20 files is way too many/few)
    if new_files:
        print("\nWould ingest:")
        for p in new_files[:20]:
            print(f"  {p}")
        if len(new_files) > 20:
            print(f"  ... ({len(new_files) - 20} more)")

    # Prune preview (read-only; no lock)
    if args.prune:
        # ingester.prune_preview returns list of dict/tuples describing candidates for pruning
        # (i.e. items with no associated raw source file)
        candidates = ingester.prune_preview(resolved.db_path, resolved.raw_dir)
        print(f"\nPrune candidates (missing raw file): {len(candidates)}")
        for c in candidates[:20]:
            print(f"  {c}")
        if len(candidates) > 20:
            print(f"  ... ({len(candidates) - 20} more)")

        if candidates and not args.apply:
            print("\nDry-run: nothing deleted (use --prune --apply to delete).")

    # APPLY section (destructive operations) under lock
    # Decides whether this run will need to modify the database; if...
    #   prune + apply -> deletes invalid items
    #   ingest + apply -> inserts new items
    needs_write = (args.apply and args.prune) or (args.apply and len(new_files) > 0)

    # Lock database (prevent simultaneous write)
    if needs_write:
        purpose_bits = []
        if args.prune:
            purpose_bits.append(f"prune {args.type}")
        if new_files:
            purpose_bits.append(f"ingest {args.type}")
        purpose = "; ".join(purpose_bits) or "ingest"

        try:
            lock = acquire_lock(resolved.db_path, purpose=purpose)
        except DatabaseLockedError as e:
            print("\nERROR:", e)
            return 3

        try:
            # Apply prune
            if args.prune and args.apply:
                pruned_summary = ingester.prune_apply(resolved.db_path, resolved.raw_dir)
                print("\nPrune applied:", pruned_summary)

            # Apply ingestion
            if new_files and args.apply:
                ingest_summary = ingester.ingest_apply(resolved.db_path, resolved.raw_dir, new_files)
                print("\nIngest applied:", ingest_summary)

        finally:
            release_lock(lock)  # Remove database lock

    else:
        if args.prune and not args.apply:
            pass  # already printed dry-run note above
        if new_files and not args.apply:
            print("\nDry-run: ingestion not executed (use --apply to ingest).")

    return 0

# Runs main() when file executed directly, 
# but does nothing if imported as module
if __name__ == "__main__":
    raise SystemExit(main())
