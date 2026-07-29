# scripts/model.py
"""
Command-line dispatcher for modelling routines that build or replace derived
SQLite tables.

This command is intentionally limited to model building.  Reporting is handled
separately so that the stored model can be built once and summarised repeatedly
without rerunning or overwriting Stage 1 and Stage 2:

    python -m scripts.fire.model_report --profile tom --db fire_db

Current model types
-------------------
* inventory:
    Rebuild survey-derived count PMF and summary tables.
* room_carbon:
    Rebuild room-level direct carbon and embodied-CO2 stock tables.
* fire_emissions:
    Rebuild the deterministic Fire Emissions Stage 1 and Stage 2 tables.

Examples
--------
    python -m scripts.model --profile tom --db inventory_db --type inventory

    python -m scripts.model --profile tom --db inventory_db \
        --type room_carbon --assumed exclude

    python -m scripts.model --profile tom --db fire_db --type fire_emissions
"""

from __future__ import annotations

import argparse
from pathlib import Path

from scripts.fire.build_fire_emissions import build_fire_emissions
from scripts.inventory.build_inventory_distributions import (
    build_inventory_distributions,
)
from scripts.inventory.build_room_carbon_stock import build_room_carbon_stock
from scripts.path_config import load_local_paths_config, resolve_db_path


# Registry of modelling actions.
#
# Adding a model here automatically makes its key available under --type.  A
# model-specific branch is only needed below when that model accepts additional
# arguments, as room_carbon currently does with --assumed.
MODELLERS = {
    "inventory": build_inventory_distributions,
    "room_carbon": build_room_carbon_stock,
    "fire_emissions": build_fire_emissions,
}


def main(argv: list[str] | None = None) -> int:
    """
    Resolve a configured database path and run one modelling build.

    The selected build function is responsible for its own transaction and any
    database file lock that it requires.
    """
    parser = argparse.ArgumentParser(
        prog="model",
        description="Build derived modelling tables in a configured SQLite database.",
    )

    parser.add_argument(
        "--profile",
        required=True,
        help="Profile name from config/local_paths.yaml, for example tom.",
    )

    parser.add_argument(
        "--db",
        required=True,
        help=(
            "Database handle from config/local_paths.yaml, for example "
            "inventory_db or fire_db."
        ),
    )

    parser.add_argument(
        "--type",
        required=True,
        choices=sorted(MODELLERS.keys()),
        help="Modelling action to run.",
    )

    parser.add_argument(
        "--assumed",
        choices=["include", "exclude"],
        default="include",
        help=(
            "Whether room_carbon includes assumed_inventory contributions. "
            "Default: include. Ignored by other model types."
        ),
    )

    args = parser.parse_args(argv)
    modeller = MODELLERS[args.type]

    # Resolve the database handle through the shared local path configuration.
    config = load_local_paths_config(Path("config") / "local_paths.yaml")
    resolved = resolve_db_path(args.profile, args.db, config)

    print("Resolved paths:")
    print(f"  DB HANDLE: {args.db}")
    print(f"  TYPE:      {args.type}")
    print(f"  DB:        {resolved.db_path}")

    if not resolved.db_path.exists():
        print("\nERROR: Database file does not exist at the resolved path.")
        print("Initialise the database before running the modelling build.")
        return 2

    try:
        # room_carbon currently has one extra model-building option.  Other
        # model functions use their own documented defaults.
        if args.type == "room_carbon":
            summary = build_room_carbon_stock(
                resolved.db_path,
                assumed=args.assumed,
            )
        else:
            summary = modeller(resolved.db_path)

    except Exception as exc:
        print("\nERROR:", exc)
        return 3

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
        print(
            "  Assumed inventory:                        "
            f"{summary.get('assumed_inventory', args.assumed)}"
        )
        print(
            "  Assumed rows read:                        "
            f"{summary.get('assumed_rows', 0)}"
        )

        print("\n  Direct carbon stock output:")
        print(
            "    Contributing item rows:                 "
            f"{summary['contributing_item_rows_carbon']}"
        )
        print(
            "    Assumed rows contributing:              "
            f"{summary.get('assumed_rows_contributing_carbon', 0)}"
        )
        print(
            "    Comparison rows eligible:               "
            f"{summary.get('carbon_comparison_rows_eligible', 0)}"
        )
        print(
            "    Comparison rows added:                  "
            f"{summary.get('carbon_comparison_rows_added', 0)}"
        )
        print(
            "    Comparison rows skipped, missing comp_1: "
            f"{summary.get('carbon_comparison_rows_skipped_missing_comp_1', 0)}"
        )
        print(
            "    room_carbon_stock rows written:         "
            f"{summary['room_carbon_rows_written']}"
        )

        print("\n  Embodied CO2 replacement output:")
        print(
            "    Contributing item rows:                 "
            f"{summary['contributing_item_rows_embodied']}"
        )
        print(
            "    Assumed rows contributing:              "
            f"{summary.get('assumed_rows_contributing_embodied', 0)}"
        )
        print(
            "    Comparison rows eligible:               "
            f"{summary.get('embodied_comparison_rows_eligible', 0)}"
        )
        print(
            "    Comparison rows added:                  "
            f"{summary.get('embodied_comparison_rows_added', 0)}"
        )
        print(
            "    Comparison rows skipped, missing comp_1: "
            f"{summary.get('embodied_comparison_rows_skipped_missing_comp_1', 0)}"
        )
        print(
            "    room_embodied_CO2 rows written:         "
            f"{summary['room_embodied_CO2_rows_written']}"
        )

    else:
        # Fire Emissions and future models can use this generic printer.  Their
        # build functions already return descriptive key/value summaries.
        for key, value in summary.items():
            print(f"  {key}: {value}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
