# scripts/model.py
"""
Command-line dispatcher for modelling routines that write derived tables
to the shared inventory SQLite database.

This mirrors the role of ingest.py, but for modelling rather than raw-data ingest.
The purpose is to keep:
    (i) generic CLI handling in one place
    (ii) model-specific logic inside dedicated build_[x].py scripts

Current model types:
    - inventory   : rebuilds survey-derived count PMF / summary tables
    - room_carbon : rebuilds room-level carbon stock summary table

Expected workflow:
    1) User has already initialised the SQLite database
    2) User has already ingested the relevant source data
    3) User runs this script to build intermediate modelling tables

Examples:
    python -m scripts.model --profile tom_test --db inventory_db --type inventory
    python -m scripts.model --profile tom_test --db inventory_db --type room_carbon
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

import yaml

from scripts.build_inventory_distributions import build_inventory_distributions
from scripts.build_room_carbon_stock import build_room_carbon_stock


@dataclass(frozen=True)
class ResolvedModelPaths:
    """
    Resolved filesystem paths needed for modelling actions.

    Kept separate from the ingest resolver because modelling does not need
    a raw input directory - only the target database path.
    """
    db_handle: str
    db_path: Path


# Registry of available modelling actions.
# Mirrors the INGESTERS pattern used in ingest.py, so future model types
# can be added in one obvious place.
MODELLERS = {
    "inventory": build_inventory_distributions,
    "room_carbon": build_room_carbon_stock,
}


# Public function
def load_local_paths_config(config_path: Path) -> dict:
    """
    Load YAML config (local paths).

    Checks config/local_paths.yaml exists
    Parses YAML into a Python dictionary

    Shared modelling assumptions:
    - profiles.<name>.sharepoint_root points to the local SharePoint/OneDrive root
    - db_roots.<db_handle>.root points to the DB folder within that root
    - db_roots.<db_handle>.rel_db points to the SQLite database path within that DB folder
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
def resolve_model_db_path(profile: str, db_handle: str, config: dict) -> ResolvedModelPaths:
    """
    Resolve the full SQLite database path for modelling workflows.

    This is intentionally separate from the ingest resolver because modelling
    does not need a raw_dir. It only needs:
      - the selected profile
      - the selected database handle
      - the resolved SQLite database path

    Expected config shape (example):
      profiles:
        tom:
          sharepoint_root: "C:/Users/.../Fire-Emissions-Databases"

      db_roots:
        inventory_db:
          root: "inventory_db"
          rel_db: "database/pooled_inventory.sqlite"
    """
    profiles = config.get("profiles", {})   # returns the selected profile
    db_roots = config.get("db_roots", {})   # returns the selected DB root directory

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

    rel_db = db_cfg.get("rel_db")
    if not rel_db:
        raise KeyError(f"Missing required db_roots.{db_handle}.rel_db in config.")

    # Create full database filepath
    db_path = sharepoint_root / Path(root) / Path(rel_db)

    return ResolvedModelPaths(
        db_handle=db_handle,
        db_path=db_path,
    )


def main(argv: list[str] | None = None) -> int:
    """
    Command-line entry point for the modelling dispatcher.

    Resolves the target database path from local_paths.yaml,
    selects the requested modelling action, and executes it.

    Unlike ingest.py, there is currently no scan/plan/apply split here:
    modelling actions are explicit rebuild operations requested by the user.
    """
    parser = argparse.ArgumentParser(
        prog="model",
        description="Build derived modelling tables in the shared inventory SQLite database.",
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
        choices=sorted(MODELLERS.keys()),
        help="Modelling action to run (e.g. inventory, room_carbon).",
    )

    # Build modelling routine
    args = parser.parse_args(argv)

    modeller = MODELLERS[args.type]

    # Load config + resolve DB path (from local_paths.yaml)
    config = load_local_paths_config(Path("config") / "local_paths.yaml")
    resolved = resolve_model_db_path(args.profile, args.db, config)

    print("Resolved paths:")
    print(f"  DB HANDLE: {args.db}")
    print(f"  TYPE:      {args.type}")
    print(f"  DB:        {resolved.db_path}")

    if not resolved.db_path.exists():
        print("\nERROR: Database file does not exist at resolved path.")
        print("Create it first by running init_db.py against the SharePoint-synced DB path.")
        return 2  # Error code: incorrect usage / invalid invocation

    # Execute requested modelling action.
    # The called script is responsible for any write-locking it requires.
    try:
        summary = modeller(resolved.db_path)
    except Exception as e:
        print("\nERROR:", e)
        return 3

    # Descriptive print summary
    print("\nModel applied successfully:")

    if args.type == "inventory":
        print(f"  Item groups processed:     {summary['item_groups']}")
        print(f"  Item PMF rows written:     {summary['item_pmf_rows']}")
        print(f"  Item summary rows written: {summary['item_summary_rows']}")
        print(f"  Room groups processed:     {summary['room_groups']}")
        print(f"  Room PMF rows written:     {summary['room_pmf_rows']}")
        print(f"  Room summary rows written: {summary['room_summary_rows']}")

    elif args.type == "room_carbon":
        print(f"  Source rows read:          {summary['source_rows']}")
        print(f"  Contributing item rows:    {summary['contributing_item_rows']}")
        print(f"  Room summary rows written: {summary['room_rows_written']}")

    else:
        # Defensive fallback in case new model types are added before
        # custom reporting text is written here.
        print("  Summary:")
        for key, value in summary.items():
            print(f"    {key}: {value}")

    return 0


# Runs main() when file executed directly,
# but does nothing if imported as module
if __name__ == "__main__":
    raise SystemExit(main())