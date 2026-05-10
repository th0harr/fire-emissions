from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class ResolvedPaths:
    """Paths resolved from profile + db_handle + config/local_paths.yaml."""

    db_path: Path
    db_handle: str
    raw_dir: Path


def load_local_paths_config(config_path: Path) -> dict:
    """
    Load YAML config/local_paths.yaml.

    Checks that the config file exists, then parses it into a Python dictionary.
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


def resolve_paths(
    profile: str,
    db_handle: str,
    ingest_type: str,
    config: dict,
) -> ResolvedPaths:
    """
    Resolve full local paths for:
      - the selected database
      - the raw directory for the chosen ingest/source type
    """
    profiles = config.get("profiles", {})
    db_roots = config.get("db_roots", {})
    paths = config.get("paths", {})

    if profile not in profiles:
        raise KeyError(
            f"Profile '{profile}' not found in config.\n"
            f"Available profiles: {', '.join(sorted(profiles.keys())) or '(none)'}"
        )

    if db_handle not in db_roots:
        raise KeyError(
            f"DB handle '{db_handle}' not found in config.\n"
            f"Available db handles: {', '.join(sorted(db_roots.keys())) or '(none)'}"
        )

    sharepoint_root = Path(profiles[profile]["sharepoint_root"])

    db_cfg = db_roots[db_handle]

    root = db_cfg.get("root")
    if not root:
        raise KeyError(f"Missing required db_roots.{db_handle}.root in config.")

    rel_db = db_cfg.get("rel_db")
    if not rel_db:
        raise KeyError(f"Missing required db_roots.{db_handle}.rel_db in config.")

    if ingest_type not in paths:
        raise KeyError(
            f"Path type '{ingest_type}' not found in config.paths.\n"
            f"Available path types: {', '.join(sorted(paths.keys())) or '(none)'}"
        )

    raw_types = db_cfg.get("raw_types", [])
    if raw_types and ingest_type not in raw_types:
        raise ValueError(
            f"Ingest type '{ingest_type}' is not allowed for db '{db_handle}'.\n"
            f"Allowed raw types: {', '.join(raw_types)}"
        )

    rel_raw = paths[ingest_type].get("rel_raw")
    if not rel_raw:
        raise KeyError(
            f"Missing required paths.{ingest_type}.rel_raw in config."
        )

    db_path = sharepoint_root / Path(root) / Path(rel_db)
    raw_dir = sharepoint_root / Path(root) / Path(rel_raw)

    return ResolvedPaths(
        db_handle=db_handle,
        db_path=db_path,
        raw_dir=raw_dir,
    )