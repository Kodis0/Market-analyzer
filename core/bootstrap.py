"""
Bootstrap: config loading, env validation, path resolution.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from core.config import AppConfig

log = __import__("logging").getLogger("bootstrap")


def load_config(cfg_path: str) -> tuple[AppConfig, dict[str, Any], Path]:
    """
    Load config from YAML. Returns (AppConfig, raw_dict, cfg_dir).
    """
    load_dotenv()
    cfg_path = os.path.abspath(cfg_path)
    cfg_dir = Path(os.path.dirname(cfg_path))

    with open(cfg_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    cfg = AppConfig.model_validate(raw)
    return cfg, raw or {}, cfg_dir


def get_paths(cfg_dir: Path, raw: dict[str, Any]) -> tuple[Path, Path, Path]:
    """
    Resolve paths. Returns (settings_path, api_db_path, quarantine_path).
    """
    settings_path = cfg_dir / "settings.json"
    api_db_path = cfg_dir / "request_stats.db"

    quarantine_path = raw.get("quarantine_path") if isinstance(raw, dict) else None
    if not quarantine_path:
        quarantine_path = "quarantine.yaml"
    if isinstance(quarantine_path, str) and quarantine_path and not os.path.isabs(quarantine_path):
        quarantine_path = cfg_dir / quarantine_path
    else:
        quarantine_path = Path(quarantine_path) if quarantine_path else cfg_dir / "quarantine.yaml"

    return settings_path, api_db_path, quarantine_path


def require_env(*keys: str) -> dict[str, str]:
    """
    Require env vars. Raises RuntimeError if any missing.
    Returns dict of key -> value.
    """
    result = {}
    for key in keys:
        val = os.environ.get(key)
        if not val:
            raise RuntimeError(f"Нет {key} в .env")
        result[key] = val
    return result
