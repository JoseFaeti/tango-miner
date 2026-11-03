"""Run the corresponding repository script as a module."""
from runpy import run_module

run_module("scripts.score_sentence_candidates", run_name="__main__")
