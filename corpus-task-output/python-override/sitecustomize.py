import os
from pathlib import Path
import appdirs
_original = appdirs.user_cache_dir
def _user_cache_dir(appname=None, *args, **kwargs):
    if appname == 'tango_miner':
        return str(Path(os.environ['TANGO_TASK_CACHE']).resolve())
    return _original(appname, *args, **kwargs)
appdirs.user_cache_dir = _user_cache_dir
