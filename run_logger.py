import json
import os
from datetime import datetime, timezone

from config_loader import load_config


class RunLogger:
    def __init__(self, script_name):
        self.script_name = script_name
        cfg = load_config()
        self.log_dir = cfg["paths"]["run_log_dir"]
        os.makedirs(self.log_dir, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.log_path = os.path.join(self.log_dir, f"{script_name}_{stamp}.jsonl")

    def _write(self, level, event, details=None):
        record = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "level": level,
            "script": self.script_name,
            "event": event,
            "details": details or {},
        }
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=True) + "\n")

    def info(self, event, details=None):
        self._write("INFO", event, details)

    def warning(self, event, details=None):
        self._write("WARNING", event, details)

    def error(self, event, details=None):
        self._write("ERROR", event, details)
