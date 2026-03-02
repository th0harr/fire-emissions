from __future__ import annotations

import argparse  # command-line argument parsing
from dataclasses import dataclass  # simple classes
from pathlib import Path  # safe file path handling on Windows
import yaml  # pyyaml

# Uses db_lock.py for file locking (prevent simultaneous write)
from scripts.db_lock import acquire_lock, release_lock, DatabaseLockedError

# Import source_type ingester modules (add more later)
from scripts import ingest_showroom_xlsx
from scripts import ingest_survey_export
from scripts import ingest_vocab

INGESTERS = {
    "showroom": ingest_showroom_xlsx,
    "survey": ingest_survey_export,
    # "insurance": ingest_insurance_data,
    "vocab": ingest_vocab,
}


# Container for current file paths
@dataclass(frozen=True)
class ResolvedPaths:
    """Paths resolved from profile + config."""
    db_path: Path
    raw_dir: Path

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
def resolve_paths(profile: str, ingest_type: str, config: dict) -> ResolvedPaths:
    """
    Relates to config/local_paths.yaml
    Resolve full local paths for:
      - the inventory DB (shared across types)
      - the raw directory for the chosen source type

    Expected config shape (example):
      profiles:
        tom:
          sharepoint_root: "C:/Users/.../University of Edinburgh"
      paths:
        inventory_db:
          rel_db: "Carbon accounting .../inventory_db/database/pooled_inventory.sqlite"
        showroom:
          rel_raw: "Carbon accounting .../inventory_db/raw/showrooms"
        survey:
          rel_raw: "Carbon accounting .../inventory_db/raw/surveys"
        insurance:
          rel_raw: "Carbon accounting .../inventory_db/raw/insurance"
    """
    profiles = config.get("profiles", {})   # returns the selected profile
    paths = config.get("paths", {})         # returns the relevant path

    if profile not in profiles:
        raise KeyError(
            f"Profile '{profile}' not found in config.\n"
            f"Available profiles: {', '.join(sorted(profiles.keys())) or '(none)'}"
        )

    sharepoint_root = Path(profiles[profile]["sharepoint_root"])

    inv = paths.get("inventory_db", {})
    rel_db = inv.get("rel_db")
    if not rel_db:
        raise KeyError("Missing required paths.inventory_db.rel_db in config.")

    type_cfg = paths.get(ingest_type, {})
    rel_raw = type_cfg.get("rel_raw")
    if not rel_raw:
        raise KeyError(
            f"Missing required paths.{ingest_type}.rel_raw in config."
        )

    # Create full ingest paths for database and source
    db_path = sharepoint_root / Path(rel_db)
    raw_dir = sharepoint_root / Path(rel_raw)

    return ResolvedPaths(db_path=db_path, raw_dir=raw_dir)


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
        "--type",
        required=True,
        choices=sorted(INGESTERS.keys()),
        help="Raw data type to ingest (e.g. showroom).",
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
    resolved = resolve_paths(args.profile, args.type, config)

    print("Resolved paths:")
    print(f"  TYPE: {args.type}")
    print(f"  DB:   {resolved.db_path}")
    print(f"  RAW:  {resolved.raw_dir}")

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
