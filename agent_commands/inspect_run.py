"""Run the corresponding repository script as a module."""
from runpy import run_module
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
run_module("scripts.inspect_run", run_name="__main__")
