import json
import os

from config_loader import load_config


def _checkpoint_path():
    cfg = load_config()
    return cfg["paths"]["checkpoint_file"]


def load_checkpoint():
    path = _checkpoint_path()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_checkpoint(data):
    path = _checkpoint_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)
    os.replace(tmp_path, path)
