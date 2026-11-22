"""Shared location for application cache files."""

from pathlib import Path
import appdirs

CACHE_DIR = Path(appdirs.user_cache_dir("tango_miner"))


def remove_legacy_entries(directory, suffix):
    """Remove obsolete content-hash cache entries after a replacement is saved."""
    try:
        for path in directory.glob('*' + suffix):
            if len(path.stem) == 64 and all(c in '0123456789abcdef' for c in path.stem):
                try:
                    path.unlink()
                except OSError:
                    pass
    except OSError:
        pass
