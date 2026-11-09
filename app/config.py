from __future__ import annotations
import os, yaml
from pathlib import Path

def load_config():
    path = Path(os.environ.get("ASADS_CONFIG", "config.yaml"))
    if not path.exists():
        raise FileNotFoundError(f"{path} not found. Copy config.example.yaml to config.yaml first.")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
