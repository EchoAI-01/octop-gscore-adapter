"""Import the plugin as a package without running Octop's plugin setup."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]
PACKAGE_NAME = "gscore_connector_under_test"


def load_plugin_package():
    module = sys.modules.get(PACKAGE_NAME)
    if module is not None:
        return module
    spec = importlib.util.spec_from_file_location(
        PACKAGE_NAME,
        PLUGIN_DIR / "main.py",
        submodule_search_locations=[str(PLUGIN_DIR)],
    )
    if spec is None or spec.loader is None:
        raise ImportError("cannot create plugin package spec")
    module = importlib.util.module_from_spec(spec)
    module.__package__ = PACKAGE_NAME
    module.__path__ = [str(PLUGIN_DIR)]
    sys.modules[PACKAGE_NAME] = module
    spec.loader.exec_module(module)
    return module
