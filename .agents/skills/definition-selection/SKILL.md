---
name: definition-selection
description: Investigate or improve Japanese candidate definitions and unresolved dictionary selections using source and component evidence.
---

# Definition selection

Follow AGENTS.md and the required mining guidance. Inspect the candidate and examples in a completed archive with `python -X utf8 scripts/inspect_run.py RUN --word WORD --occurrences --examples 2`; use `scripts/query_jsonl.py` for targeted diagnostic records when available. Check the original source, surface, reading, part of speech and component evidence against JMdict spelling, reading and sense restrictions. Do not assume an archived selected entry identifies the correct sense or that missing diagnostics prove an absence.

Trace `src/steps/AddDefinitionsStep.py`, `src/JMDict.py`, and example selection as relevant. Component counts influence contextual selection; some derived potential definitions have no selected entry recorded. Check whether the mismatch arises upstream in tokenization, merging, target or example selection rather than assuming dictionary ranking is responsible. Preserve ambiguity when evidence is insufficient and retain useful candidate and component evidence.

For a behavioral change, add focused regressions for the misleading definition or unresolved case plus valid homophones, senses, source forms and lexical candidates that must remain. Assess example compatibility and recall. Follow the full test and documentation requirements in AGENTS.md. Use candidate-investigation for broader pipeline tracing.
