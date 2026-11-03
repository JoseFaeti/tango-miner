"""Run the corresponding repository script as a module."""
from runpy import run_module

run_module("scripts.check_jmdict", run_name="__main__")
