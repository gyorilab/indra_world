__version__ = '1.1.0'
from pathlib import Path
from indra.config import get_config

default_bucket = 'world-modelers'
default_key_base = 'indra_models'
default_profile = 'wm'
cache_config = get_config('INDRA_WM_CACHE')
if cache_config:
    CACHE = Path(cache_config)
    CACHE.mkdir(exist_ok=True)
else:
    CACHE = None
