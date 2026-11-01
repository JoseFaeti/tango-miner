---
name: grammar
description: Implement or extend a Japanese grammar card in Tango Miner with focused matching, regression tests, and documentation.
---

# Tango Miner grammar cards

Follow AGENTS.md. Implement one grammar point per task unless explicitly requested otherwise.

## 1. Select the grammar point

If the task specifies a grammar point, use it.

Otherwise, find the first unchecked entry in "docs/grammar-priorities.md". Read only the relevant section initially.

Consult "docs/grammar/remaining-candidates.md" only when the priority queue is exhausted or the selected point requires clarification.

## 2. Inspect relevant context

Use targeted searches and bounded file reads. Avoid dumping entire documentation files, directories, or test suites.

Initially inspect only:

- The relevant grammar rule interface and registry.
- One or two similar existing rules.
- Their corresponding tests.
- Relevant sections of README.md and docs/false-positive-analysis.md.

Expand investigation only when required by ambiguity, unexpected tokenization, failed tests, or a potential regression.

Prefer bounded scripts such as "scripts/replay_grammar.py", "scripts/query_jsonl.py", "scripts/inspect_run.py", and "scripts/compare_runs.py" when source evidence is needed.

Avoid repeatedly reading files whose relevant contents are already available in context.

## 3. Define matching behavior

Determine the intended grammatical meaning, supported forms, and likely false positives.

Use actual source-aligned Sudachi tokenizations when necessary to establish matching behavior.

Preserve useful lexical readings and attached-word evidence. Match complete token boundaries and account for supported inflections and contractions.

Avoid broad filters and unsupported assumptions.

Investigate dictionary precedence, grammar reservation, and lexical frequency effects when the proposed implementation could affect them.

Do not equate a higher match count with improved precision or recall.

## 4. Implement

- Add the focused rule under "src/grammar_rules/rules/".
- Register it in "src/grammar_rules/registry.py".
- Add its stable identity to "src/GrammarConstants.py".
- Reuse existing shared helpers where appropriate.
- Inspect grammar caching dependencies only when adding or changing dependencies.

Prefer the smallest implementation consistent with the established architecture.

## 5. Test

Add focused regression tests covering:

- Supported grammatical forms.
- Known ambiguous or misleading forms.
- Relevant lexical controls.
- Grammar reservation, examples, and identity integration where applicable.

Run focused tests first.

Fix failures and rerun the affected tests.

Run "python scripts/run_tests.py" once after focused tests pass. Rerun the full suite only if subsequent changes could affect it.

Do not mutate real Anki data.

Keep successful test output concise. Inspect detailed output only for failures.

## 6. Documentation and verification

Update only the relevant portions of:

- README.md
- "docs/grammar-priorities.md"
- The grammar card documentation

Describe actual supported forms, limitations, and uncertain interpretations.

Preserve existing line endings.

Inspect the final diff and report the files changed, tests run, and any remaining limitations.

## 7. Corpus pipeline safety

Do not start a full corpus pipeline run without explicit user authorization.

For an authorized run:

- Agree on the curated input corpus.
- Verify that Anki is disabled or output-only mode is configured.
- Save verbose output to a log.
- Wait for process completion before inspecting results.
- Use bounded inspection and comparison scripts.
- Report failures, Anki status, and observed counts separately from independent precision or recall estimates.

If the user runs the pipeline, inspect its completed archive when requested.

## 8. Efficiency

Minimize unnecessary context consumption without compromising correctness.

- Prefer targeted searches over full-file reads.
- Avoid redundant repository exploration.
- Do not inspect unrelated grammar rules or historical archives without a concrete reason.
- Avoid repeated full-suite test runs.
- Keep command output bounded.
- Do not perform speculative refactoring or unrelated cleanup.
- Expand investigation when evidence suggests the initial scope is insufficient.