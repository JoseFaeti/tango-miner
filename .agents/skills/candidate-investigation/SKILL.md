---
name: candidate-investigation
description: Investigate missing, misleading, or unexpected Japanese mining candidates by tracing source evidence through the pipeline.
---

# Candidate investigation

Follow AGENTS.md; read the required project guidance before changing mining behavior. Start with the reported source and a completed archive if available. Use `python -X utf8 scripts/inspect_run.py RUN --word WORD --occurrences --rejected-merges --examples 2` and `scripts/query_jsonl.py --help` for bounded diagnostic searches. Diagnostics are optional and absent from older runs; absence of a record is not proof that the source lacks an occurrence. See docs/usage/development.md for artifact meanings and limitations.

Trace the actual source-aligned tokenization, merging or grammar reservation, collection eligibility, frequency and source coverage, definitions, and selected example as needed. Search the relevant `src/steps/` and tests for the stage implicated by evidence. Occurrences include ineligible tokens; eligible occurrences absent from candidate diagnostics may have failed frequency filtering. Rejected merges cover only matches actually considered. Preserve component evidence and distinguish stored provenance from evidence actually used in decisions.

For changes, include source-grounded false-positive cases and useful lexical or ambiguous controls; assess recall and the existing standalone proper-noun filtering gap. Report the point of failure, evidence and remaining uncertainty. Use the grammar or definition-selection skill when that specialized workflow applies. Do not run the full pipeline without the corpus agreement and Anki safeguards in AGENTS.md.
