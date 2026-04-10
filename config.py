import json
from pathlib import Path

DEFAULT_CONFIG_PATH = Path(__file__).parent / "ipl_spotter_config.json"


def load_config(path: str = None) -> dict:
    path = path or str(DEFAULT_CONFIG_PATH)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[IPL-SPOTTER] Config not found: {path}")
        return {}
    except json.JSONDecodeError as exc:
        print(f"[IPL-SPOTTER] Config parse error: {exc}")
        return {}
