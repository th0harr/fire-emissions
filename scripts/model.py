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
    python -m scripts.model --profile tom_test --db inventory_db --type room_carbon --assumed exclude
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path

from scripts.path_config import load_local_paths_config, resolve_db_path
from scripts.inventory.build_inventory_distributions import build_inventory_distributions
from scripts.inventory.build_room_carbon_stock import build_room_carbon_stock



# Registry of available modelling actions.
# Mirrors the INGESTERS pattern used in ingest.py, so future model types
# can be added in one obvious place.
MODELLERS = {
    "inventory": build_inventory_distributions,
    "room_carbon": build_room_carbon_stock,
}


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

    parser.add_argument(
        "--assumed",
        choices=["include", "exclude"],
        default="include",
        help=(
            "Whether to include assumed_inventory contributions when running "
            "the room_carbon model. Default: include. Ignored by other model types."
        ),
    )


    # Build modelling routine
    args = parser.parse_args(argv)

    modeller = MODELLERS[args.type]

    # Load config + resolve DB path (from local_paths.yaml)
    config = load_local_paths_config(Path("config") / "local_paths.yaml")
    resolved = resolve_db_path(args.profile, args.db, config)

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
    #
    # room_carbon has one extra sensitivity option:
    #   --assumed include  -> include assumed_inventory rows in the room carbon stock
    #   --assumed exclude  -> ignore assumed_inventory rows
    #
    # For now, this option is only passed to build_room_carbon_stock().
    # Other model types ignore it.
    try:
        if args.type == "room_carbon":
            summary = build_room_carbon_stock(
                resolved.db_path,
                assumed=args.assumed,
            )
        else:
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
        print(f"  Source rows read:                         {summary['source_rows']}")
        print(f"  Assumed inventory:                        {summary.get('assumed_inventory', args.assumed)}")
        print(f"  Assumed rows read:                        {summary.get('assumed_rows', 0)}")

        print("\n  Direct carbon stock output:")
        print(f"    Contributing item rows:                 {summary['contributing_item_rows_carbon']}")
        print(f"    Assumed rows contributing:              {summary.get('assumed_rows_contributing_carbon', 0)}")
        print(f"    Comparison rows eligible:               {summary.get('carbon_comparison_rows_eligible', 0)}")
        print(f"    Comparison rows added:                  {summary.get('carbon_comparison_rows_added', 0)}")
        print(
            "    Comparison rows skipped, missing comp_1: "
            f"{summary.get('carbon_comparison_rows_skipped_missing_comp_1', 0)}"
        )
        print(f"    room_carbon_stock rows written:         {summary['room_carbon_rows_written']}")

        print("\n  Embodied CO2 replacement output:")
        print(f"    Contributing item rows:                 {summary['contributing_item_rows_embodied']}")
        print(f"    Assumed rows contributing:              {summary.get('assumed_rows_contributing_embodied', 0)}")
        print(f"    Comparison rows eligible:               {summary.get('embodied_comparison_rows_eligible', 0)}")
        print(f"    Comparison rows added:                  {summary.get('embodied_comparison_rows_added', 0)}")
        print(
            "    Comparison rows skipped, missing comp_1: "
            f"{summary.get('embodied_comparison_rows_skipped_missing_comp_1', 0)}"
        )
        print(f"    room_embodied_CO2 rows written:         {summary['room_embodied_CO2_rows_written']}")

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