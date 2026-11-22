"""Shared location for application cache files."""

from pathlib import Path

CACHE_DIR = Path(__file__).resolve().parent.parent / ".cache"
