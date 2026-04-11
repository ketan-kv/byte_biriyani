from __future__ import annotations

from pathlib import Path

import yaml


DEFAULT_CONFIG_PATH = Path("config/config.yaml")


def load_config(config_path: str | Path = DEFAULT_CONFIG_PATH) -> dict:
    path = Path(config_path)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return data or {}
