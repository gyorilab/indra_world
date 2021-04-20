from pathlib import Path
from indra.config import get_config

default_bucket = 'world-modelers'
default_key_base = 'indra_models'
file_defaults = {'raw': 'raw_statements',
                 'sts': 'statements',
                 'cur': 'curations',
                 'meta': 'metadata'}


default_profile = 'wm'
cache_config = get_config('INDRA_WM_CACHE')
if cache_config:
    CACHE = Path(cache_config)
    CACHE.mkdir(exist_ok=True)
else:
    CACHE = None


class InvalidCorpusError(Exception):
    pass


from .corpus import Corpus
from .curator import LiveCurator
