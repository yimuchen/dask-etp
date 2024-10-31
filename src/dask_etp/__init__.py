from .cluster import ETPCondorCluster

# Ensuring that our custom configurations are used by default
from .config import _ensure_user_config_file, _set_base_config

_ensure_user_config_file()
_set_base_config()
