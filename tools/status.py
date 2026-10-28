"""Print the live GsCore connector status file."""

from __future__ import annotations

import json
import sys
from pathlib import Path

TOOLS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(TOOLS_DIR))

from _plugin_loader import load_plugin_package  # noqa: E402

load_plugin_package()
config_mod = __import__("gscore_connector_under_test.config", fromlist=["x"])


def main() -> int:
    config_path = config_mod.default_config_path()
    status_path = Path(sys.argv[1]) if len(sys.argv) > 1 else config_mod.default_status_path(config_path)
    if not status_path.is_file():
        print(f"status file not found: {status_path}")
        print(f"config file: {config_path}")
        return 1
    print(status_path.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
