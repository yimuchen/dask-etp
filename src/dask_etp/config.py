import os
from pathlib import Path

import dask
import yaml

PKG_CONFIG_FILE = Path(__file__).parent / "jobqueue-kitetp.yaml"


def _ensure_user_config_file():
    dask.config.ensure_file(source=PKG_CONFIG_FILE)


def _set_base_config(priority: str = "old"):
    with open(PKG_CONFIG_FILE) as f:
        defaults = yaml.safe_load(f)
    dask.config.update(dask.config.config, defaults, priority=priority)
