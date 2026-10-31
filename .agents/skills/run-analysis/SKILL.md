---
name: run-analysis
description: Inspect or compare completed Tango Miner run archives, including candidates and diagnostic evidence, without rerunning mining.
---

# Run analysis

Use `python tango_miner.py report` subcommands to query summary, warnings, diff, Anki or performance for a selected run; use `--help` for options. Read `report.md` or detailed JSON only when needed. Use completed archives and the bounded tools in docs/usage/development.md. `python -X utf8 scripts/inspect_run.py RUN --word WORD --occurrences --examples 2` inspects a candidate; add `--rejected-merges` where relevant. `python -X utf8 scripts/compare_runs.py BEFORE AFTER --compact --word WORD --limit 5` compares runs. Omit both run paths only when the two most recent completed runs are intended. `scripts/query_jsonl.py` searches a diagnostic field and streams the file; consult `--help` for exact options. Bound printed results; scans may still read the entire JSONL file.

Compare input manifests, configuration, dictionary and analyzer evidence before attributing changes. Optional diagnostics may be absent. Occurrence totals include ineligible tokens; dropped/review output combines invalid and unresolved cases. An output removal does not establish an Anki deletion, and candidate counts alone do not measure precision or recall. Inspect source examples and useful missing candidates before drawing conclusions. Source text and paths in artifacts may be private.

Inspection and comparison do not run mining or contact Anki. If a new full run is requested, follow AGENTS.md: agree on the curated corpus, verify resolved Anki settings, log verbose output, wait for completion and report failures. Do not claim a corpus run is authorized merely because archive analysis is requested.
