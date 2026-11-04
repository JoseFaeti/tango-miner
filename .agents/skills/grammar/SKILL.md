---
name: grammar
description: Implement or extend a Japanese grammar card in Tango Miner with focused matching, regression tests, and documentation.
---

# Tango Miner grammar cards

Follow AGENTS.md, including its documentation scope, recall, safety and test
rules. Implement one point per task unless explicitly requested otherwise.

## Select and inspect

For an explicit point, find its specification in `docs/grammar/points/`.
Otherwise run `python scripts/next_grammar.py` and read the returned specification.
If it returns `null`, select the first `[~]` in
`docs/grammar/partially-implemented.md` when further work is appropriate.
For a partial point identify supported behavior and remaining work. Consult
`later.md` only when the queue
is exhausted or it is relevant to the point.

Initially inspect the relevant sections of `src/grammar_rules/base.py`,
`src/grammar_rules/registry.py`, `src/GrammarConstants.py`, and the closest rule
and its test. Find symbols with targeted searches and read bounded sections;
avoid printing whole large files or the complete development guide when a
relevant section or command reference will suffice. If no closer
example is apparent, start with `src/grammar_rules/rules/tai/index.py` and
`src/grammar_rules/rules/tai/test_index.py`; choose another when its behavior better fits the
point. Expand only for a concrete reason, such as ambiguity, missing evidence,
unexpected tokenization, a failed test, or a potential regression. Use bounded
reads and existing scripts such as `scripts/replay_grammar.py` when useful.

## Define and implement

Determine meaning, supported forms, ambiguous readings and lexical controls.
Use actual source-aligned Sudachi tokenizations when needed. Match complete token
boundaries; account for supported inflections and contractions, preserve useful
lexical readings and attached-word evidence, and avoid unsupported assumptions.
Investigate dictionary precedence, grammar reservation, lexical frequency and
caching dependencies when the change can affect them. Match counts alone do not
establish precision or recall.

Add or extend a focused rule under `src/grammar_rules/rules/`, register it in
`src/grammar_rules/registry.py`, and add a stable identity to
`src/GrammarConstants.py` where needed. Reuse shared helpers where appropriate.

## Test and document

Add focused regressions for supported forms, ambiguous or misleading forms,
lexical controls, and reservation, examples and identity integration when
applicable. Run focused tests through `python scripts/run_tests.py` first; fix
failures, then run the full suite once. Repeat it when later changes could affect
it. Wait for the test process to finish and check its exit status; do not poll
while a blocking wait is available. If the tool imposes a timeout, resume
waiting only after it expires, using the longest permitted wait.
Follow AGENTS.md for integration dependencies and Anki safety.

| Document | Action |
| --- | --- |
| Point specification | Always record verified behavior, forms, tests, evidence, ambiguities and remaining limitations; use one canonical description. |
| Point index | Keep unstarted `[ ]` entries in `next.md`. When work makes a point partial, change its specification to `[~]`, remove its entry from `next.md`, and add it once as `[~]` in `partially-implemented.md`, preserving the existing order convention. Move it to `implemented.md` as `[x]` only when the scheduled scope is fully implemented and validated; keep the specification and index statuses consistent. |
| Shared grammar pages | Inspect and update the relevant page when its stated precedence, identity, inflection or other shared behavior is affected. |

Keep `[ ]`, `[~]` and `[x]` distinct and consistent in the specification and
index. Run `python scripts/check_grammar_docs.py`; it checks structure, not
linguistic correctness. Inspect the final diff and report tests and limitations.
