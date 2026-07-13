# scripts/fire/build_fire_events.py
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any, Optional
from collections import Counter

from scripts.fire.fire_event_resolver import (
    BlockingResolutionError,
    INPUT_TYPE_FRIS,
    insert_fire_events_and_warnings,
)
from scripts.path_config import (
    load_local_paths_config,
    resolve_db_path as resolve_db_path_from_config,
)
from scripts.fire.prep_fire_events_fris import prepare_fris_events


# -----------------------------------------------------------------------------
# Diagnostics
# -----------------------------------------------------------------------------

def print_omission_summary(events: list) -> None:
    """
    Print a compact summary of row-level omissions.

    This is mainly a diagnostic table for the FRIS resolver. It helps separate
    genuine model exclusions, such as unsupported dwelling types, from data
    quality problems, such as NULL fields in the raw FRIS row.

    This can be printed in dry-run mode because the resolver has already built
    the PreparedFireEvent objects in memory. No database write is required.
    """
    omitted_events = [
        event
        for event in events
        if str(getattr(event, "omit_from_model", "")).lower() in {"yes", "1", "true"}
    ]

    if not omitted_events:
        print()
        print("Omissions by omit_reason")
        print("------------------------")
        print("No omitted rows.")
        return

    omit_reason_counts = Counter(
        getattr(event, "omit_reason", None) or "(missing omit_reason)"
        for event in omitted_events
    )

    print()
    print("Omissions by omit_reason")
    print("------------------------")

    for omit_reason, n_events in omit_reason_counts.most_common():
        print(f"{n_events:>8}  {omit_reason}")


# -----------------------------------------------------------------------------
# Command line entry point
# -----------------------------------------------------------------------------

# This script is the user-facing dispatcher for building the model-facing
# fire_events table.
#
# The actual resolution logic is kept out of this file deliberately:
#   - prep_fire_events_fris.py handles FRIS-specific row preparation.
#   - fire_event_resolver.py contains shared validation, mapping, warning,
#     and database-writing helpers.
#
# This file should therefore stay relatively small.  Its job is to resolve the
# correct SQLite database path, run the selected input route, report the dry-run
# summary, and write the prepared rows only when --apply is used.


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    try:
        db_path = resolve_database_argument(
            db_arg=args.db,
            profile=args.profile,
            config_path=args.config,
        )

        result = build_fire_events(
            db_path=db_path,
            input_type=args.type,
            apply=args.apply,
            overwrite=args.overwrite,
            keep_omitted_events=not args.drop_omitted_events,
            run_mapping_coverage_check=not args.skip_mapping_coverage_check,
        )

    except BlockingResolutionError as exc:
        print("\nBlocking fire-event resolution error")
        print("------------------------------------")
        print(str(exc))
        return 2

    print_build_result(result)
    print_omission_summary(result["events"])
    return 0


# -----------------------------------------------------------------------------
# Command line arguments
# -----------------------------------------------------------------------------

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build model-facing fire_events rows from staged fire inputs."
    )

    parser.add_argument(
        "--profile",
        required=False,
        help=(
            "Profile name in config/local_paths.yaml, for example 'tom'. "
            "This is required when --db is a database handle such as fire_db."
        ),
    )

    parser.add_argument(
        "--db",
        required=True,
        help=(
            "Database handle from local_paths.yaml, such as fire_db, or an "
            "explicit path to a SQLite database."
        ),
    )

    parser.add_argument(
        "--config",
        default=None,
        help=(
            "Optional explicit path to local_paths.yaml.  If omitted, the "
            "script searches for config/local_paths.yaml and local_paths.yaml "
            "from the current project checkout."
        ),
    )

    parser.add_argument(
        "--type",
        choices=["fris"],
        default="fris",
        help=(
            "Input route to build.  Currently only the FRIS route is implemented."
        ),
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Write prepared rows to fire_events and fire_event_warnings.  "
            "Without this flag the script only performs a dry run."
        ),
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Delete existing model-facing rows for this input route before "
            "inserting the new build."
        ),
    )

    parser.add_argument(
        "--drop-omitted-events",
        action="store_true",
        help=(
            "Do not retain omitted incidents in fire_events.  Warnings are still "
            "inserted when --apply is used.  By default, omitted incidents are "
            "retained where the schema supports omit_from_model."
        ),
    )

    parser.add_argument(
        "--skip-mapping-coverage-check",
        action="store_true",
        help=(
            "Skip the build-level check that all present FRIS categories are "
            "covered by the mapping tables.  This is mainly for isolated tests "
            "and should not normally be used for production runs."
        ),
    )

    return parser.parse_args(argv)


# -----------------------------------------------------------------------------
# Main build function
# -----------------------------------------------------------------------------

def build_fire_events(
    *,
    db_path: str | Path,
    input_type: str = INPUT_TYPE_FRIS,
    apply: bool = False,
    overwrite: bool = False,
    keep_omitted_events: bool = True,
    run_mapping_coverage_check: bool = True,
) -> dict[str, Any]:
    """
    Build model-facing fire_events from staged inputs.

    Dry-run behaviour
    -----------------
    The default behaviour is deliberately non-destructive.  Rows are prepared,
    validated, and summarised, but no database writes are made unless --apply is
    supplied.

    This mirrors the ingest scripts used elsewhere in the project.  It means we
    can check that the resolver is happy with the current mappings before making
    any changes to fire_events.
    """
    db_path = Path(db_path)

    # Open the exact SQLite file resolved above.
    #
    # This intentionally avoids the generic ingest db_connect helper because
    # this script has already resolved the DB handle to a concrete path.
    # Using sqlite3.connect(str(db_path)) also makes PRAGMA database_list and
    # SQLiteStudio checks easier to compare while debugging.
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    try:
        conn.execute("PRAGMA foreign_keys = ON;")

        if input_type == INPUT_TYPE_FRIS:
            events, warnings, summary = prepare_fris_events(
                conn=conn,
                run_mapping_coverage_check=run_mapping_coverage_check,
            )
        else:  # pragma: no cover - guarded by argparse at present
            raise BlockingResolutionError(
                f"Unsupported fire-event input type: {input_type}"
            )

        write_result = {
            "events_inserted": 0,
            "warnings_inserted": 0,
            "omitted_events_inserted": 0,
        }

        if apply:
            conn.execute("BEGIN;")
            write_result = insert_fire_events_and_warnings(
                conn,
                events=events,
                warnings=warnings,
                overwrite=overwrite,
                input_type=input_type,
                keep_omitted_events=keep_omitted_events,
            )
            conn.commit()

        return {
            "db_path": str(db_path),
            "input_type": input_type,
            "apply": apply,
            "overwrite": overwrite,
            "keep_omitted_events": keep_omitted_events,
            "summary": summary.to_dict(),
            "write_result": write_result,
            "events": events,
        }

    except Exception:
        # If anything goes wrong during the apply stage, roll the transaction
        # back.  This keeps fire_events and fire_event_warnings in step.
        try:
            conn.rollback()
        except Exception:
            pass
        raise

    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Reporting
# -----------------------------------------------------------------------------

def print_build_result(result: dict[str, Any]) -> None:
    """
    Print a compact build summary.

    This is intentionally plain text so that it is easy to copy into notes or a
    project log.  More detailed QA tables can be added later once the first FRIS
    route is stable.
    """
    summary = result["summary"]
    write_result = result["write_result"]

    print("\nFire event build summary")
    print("------------------------")
    print(f"DB:                  {result['db_path']}")
    print(f"Input type:          {result['input_type']}")
    print(f"Mode:                {'apply' if result['apply'] else 'dry run'}")
    print(f"Overwrite:           {result['overwrite']}")
    print(f"Rows read:           {summary['rows_read']}")
    print(f"Rows prepared:       {summary['rows_prepared']}")
    print(f"Rows insertable:     {summary['rows_insertable']}")
    print(f"Rows omitted:        {summary['rows_omitted']}")
    print(f"Warnings prepared:   {summary['warnings']}")

    if result["apply"]:
        print("\nRows written")
        print("------------")
        print(f"fire_events:         {write_result['events_inserted']}")
        print(f"fire_event_warnings: {write_result['warnings_inserted']}")
        print(f"omitted retained:    {write_result['omitted_events_inserted']}")
    else:
        print("\nDry run only: no rows were written. Use --apply to write this build.")


# -----------------------------------------------------------------------------
# Path resolution
# -----------------------------------------------------------------------------

# The earlier version of this script used a lightweight local fallback and could
# therefore open the wrong SQLite file when --db fire_db was supplied.  The rest
# of the project already resolves database handles through path_config.py, so the
# build script now does the same thing.


def find_local_paths_config(config_path: Path | None = None) -> Path:
    """
    Find the local paths config used by the rest of the project.

    The fire-event builder only needs the database path, but it should still
    resolve this using the same project-level config as the ingest scripts. This
    keeps commands such as:

        --profile tom --db fire_db

    consistent across the whole pipeline.
    """
    if config_path is not None:
        config_path = Path(config_path).expanduser()
        if not config_path.exists():
            raise FileNotFoundError(f"local_paths config not found: {config_path}")
        return config_path

    search_roots: list[Path] = []

    # First search from the current working directory. This is normally the
    # project root when running python -m scripts.fire.build_fire_events.
    cwd = Path.cwd()
    search_roots.append(cwd)
    search_roots.extend(cwd.parents)

    # Then search relative to this script file. This makes the command a little
    # more robust if it is called from a different working directory.
    here = Path(__file__).resolve()
    search_roots.append(here.parent)
    search_roots.extend(here.parents)

    seen: set[Path] = set()

    for root in search_roots:
        if root in seen:
            continue
        seen.add(root)

        candidates = [
            root / "config" / "local_paths.yaml",
            root / "local_paths.yaml",
        ]

        for candidate in candidates:
            if candidate.exists():
                return candidate

    raise FileNotFoundError(
        "Could not find local_paths.yaml. Expected either:\n"
        "  config/local_paths.yaml\n"
        "or:\n"
        "  local_paths.yaml"
    )


def resolve_database_argument(
    db_arg: str,
    profile: str,
    config_path: Path | None = None,
) -> Path:
    """
    Resolve the database argument supplied on the command line.

    Important:
    ----------
    If --db is a database handle such as 'fire_db', we should resolve it through
    config/local_paths.yaml before treating it as a literal file path.

    This order matters because a local folder or file called 'fire_db' may exist
    inside the code repository. If we check Path('fire_db').exists() first, the
    script can accidentally open the wrong SQLite file/location and then report
    that all mapping tables are missing.
    """
    from scripts.path_config import (
        load_local_paths_config,
        resolve_db_path as resolve_config_db_path,
    )

    # First try to interpret --db as a configured database handle.
    #
    # This is the intended route for commands such as:
    #   --profile tom --db fire_db
    try:
        local_paths_config = find_local_paths_config(config_path)
        config = load_local_paths_config(local_paths_config)

        resolved = resolve_config_db_path(
            profile=profile,
            db_handle=db_arg,
            config=config,
        )

        return Path(resolved.db_path)
    except KeyError:
        # The value was not a configured DB handle. In that case, fall back to
        # treating it as a literal path below.
        pass

    # Fallback route for explicit database file paths.
    #
    # Examples:
    #   --db ./fire_incidents.sqlite
    #   --db C:/path/to/fire_incidents.sqlite
    candidate = Path(db_arg).expanduser()

    if candidate.exists():
        return candidate

    raise FileNotFoundError(
        f"Could not resolve database argument: {db_arg}\n\n"
        f"Tried to resolve it as a configured db_handle using local_paths.yaml, "
        f"then as a literal file path."
    )


def find_local_paths_config(config_path: Optional[str | Path] = None) -> Path:
    """
    Find local_paths.yaml for this project checkout.

    path_config.py expects to be given the config path explicitly.  Most of the
    time the project will have this at:

        config/local_paths.yaml

    During earlier development we have also used local_paths.yaml at the project
    root, so this helper checks both locations while walking up from the current
    working directory and from the script location.
    """
    if config_path:
        candidate = Path(config_path)
        if candidate.exists():
            return candidate
        raise BlockingResolutionError(f"Config file not found: {candidate}")

    start_points = [Path.cwd(), Path(__file__).resolve().parent]
    relative_candidates = [
        Path("config") / "local_paths.yaml",
        Path("local_paths.yaml"),
    ]

    for start in start_points:
        for folder in [start, *start.parents]:
            for rel in relative_candidates:
                candidate = folder / rel
                if candidate.exists():
                    return candidate

    raise BlockingResolutionError(
        "Could not find local_paths.yaml. Expected either config/local_paths.yaml "
        "or local_paths.yaml somewhere above the current working directory."
    )


def _looks_like_path(path_value: Path) -> bool:
    """
    Return True if the DB argument looks like a file path rather than a DB key.

    Handles such as fire_db should be resolved through local_paths.yaml.  Values
    containing path separators, drive letters, or SQLite-like suffixes are better
    treated as explicit paths.
    """
    value = str(path_value)

    return (
        "/" in value
        or "\\" in value
        or ":" in value
        or path_value.suffix.lower() in {".sqlite", ".sqlite3", ".db"}
    )


# Backwards-compatible alias for quick one-line checks from PowerShell, e.g.:
#   python -c "from scripts.fire.build_fire_events import resolve_db_path; print(resolve_db_path('fire_db', profile='tom'))"

def resolve_db_path(
    db_arg: str | Path,
    *,
    profile: Optional[str] = None,
    config_path: Optional[str | Path] = None,
) -> Path:
    return resolve_database_argument(
        db_arg=db_arg,
        profile=profile,
        config_path=config_path,
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
