---
name: review-evaluation
description: Sample, label, or score independently reviewed Tango Miner examples and interpret evaluation limits.
---

# Review evaluation

Use the offline workflow in docs/review-evaluation.md and `python -X utf8 scripts/evaluate_review.py SUBCOMMAND --help`. The `sample`, `label-template`, `prompts` and `score` commands operate on completed runs and do not call Anki or a model. Create output directories first; commands refuse overwrites. Record the run, seed, sample design, dictionary compatibility, and exclusions. Keep source-derived text local unless sharing is authorized.

Review source and dictionary alternatives independently before labeling; allow uncertain, unresolved and absent-from-dictionary cases. Keep labels hidden from the method being evaluated, permit abstention, and do not fabricate gold targets or entry IDs. Turn well-supported errors and useful controls into focused regressions when asked to improve behavior; suspended cards are evidence, not a blacklist.

Report labeled counts, selection coverage, errors, abstentions and limitations. Balanced candidate samples and selected examples cannot establish corpus-wide or occurrence-weighted accuracy or recall, particularly for missing candidates; matching an entry ID does not establish the correct sense or span. Follow AGENTS.md before any mining changes or full corpus run.
