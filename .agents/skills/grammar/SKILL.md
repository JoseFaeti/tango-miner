---
name: grammar
description: Implement or extend a Japanese grammar card in the Tango Miner pipeline, including source-grounded matching, recall-sensitive tests, and documentation. Use for grammar point implementation in this repository.
---

# Tango Miner grammar cards

Follow the repository's AGENTS.md. Read README.md and docs/false-positive-analysis.md before changing mining behavior; consult docs/grammar-priorities.md. For an unspecified next point, select the first unchecked entry in the tracker; use current code and tests when documentation is stale.

Inspect existing rules, tests, scripts and archive evidence before choosing a scope. Prefer the existing bounded scripts, including scripts/replay_grammar.py, scripts/query_jsonl.py, scripts/inspect_run.py and scripts/compare_runs.py. Examine actual source-aligned Sudachi tokenizations and semantic ambiguity. Decide what the grammar card means and which evidence supports it; preserve useful lexical readings and retain attached words and component evidence. Avoid broad filters. Dictionary expression precedence and grammar reservation can change lexical candidates and frequencies, so assess their effects rather than equating a larger match count with improved recall.

Implement a focused rule under src/grammar_rules/rules/, register it in src/grammar_rules/registry.py, and add its stable identity to src/GrammarConstants.py so Anki protection and legacy migration remain consistent. Use existing shared helpers where appropriate; inspect grammar caching dependencies if adding project dependencies. Match complete source-aligned token boundaries and account for supported inflections and contractions when evidence warrants them. Describe uncertain meanings honestly; unsupported forms should remain available for lexical processing.

Add focused regressions for supported forms, misleading or ambiguous analyses, useful lexical controls, and grammar reservation, examples and identity integration as appropriate. Run `python -m unittest discover -s tests` without real Anki mutations. Update README.md and the grammar tracker and card documentation to reflect the actual scope and limitations. Preserve existing line endings and inspect the diff.

Do not start a full corpus pipeline run unless the user explicitly asks you to run it. For an authorized run, first agree on the curated input corpus and verify the resolved configuration disables Anki or uses output-only mode; Save verbose output to a log. Wait for the process completion signal or exit status before inspecting the run, log or artifacts. After completion, use the bounded inspection and comparison scripts, report failures and Anki status, and distinguish observed counts from an independent precision or recall evaluation. If the user runs the pipeline, inspect its completed archive when asked.
