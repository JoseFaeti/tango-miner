# Working on Tango Miner

Read the project overview in `README.md` for context; consult its remaining
sections when relevant. Start documentation research in the active
area relevant to the task; for grammar tasks use `docs/grammar/`. `AGENTS.md`,
`README.md`, and applicable skills are also accessible. Inspect relevant source,
tests, scripts and source evidence as needed. Expand to another active
documentation area only when a specific cross-topic dependency requires it;
state the reason before expanding. Prefer targeted searches within that scope.
Never read or search `docs/archived/`, including in repository-wide searches;
exclude it explicitly if a broader search is necessary. The current code and
tests determine actual behavior when documentation is stale.

- Preserve the project's recall-first goal: useful vocabulary, grammar,
  expressions, colloquialisms, names, and uncertain candidates are in scope.
  Treat suspicious candidates as review signals and regression seeds, not a
  blacklist or automatic removal queue. Do not introduce broad filters
  based on kana, POS, length, or frequency without assessing recall loss.
- When improving a target or definition, ground it in the source and retain
  relevant component evidence. Test both corrected false positives and useful
  candidates that must remain. Do not assume stored provenance is already
  used for selection or exported.
- Existing behavior does not always meet the intended policy: standalone
  proper nouns are currently filtered. Identify such gaps explicitly and add
  focused regressions when changing them.
- For environment setup, follow the JMdict baseline instructions in
  `docs/usage/development.md` and report a missing or mismatched snapshot.
- Run `python scripts/run_tests.py` for pipeline changes. Run all tests through
  this quiet runner, passing unittest names or discovery arguments for focused
  runs; it prints failure details only and signals success through its exit code. A passing
  full suite is required: investigate and fix failures rather than accepting
  them as a baseline. Preserve meaningful regression coverage and report any
  unavailable integration dependencies. Avoid real Anki mutations during
  tests; inspect the current pipeline before running it.
- Keep documentation free of absolute local paths and personal machine details.
  Preserve the user's existing changes and update relevant documentation when
  behavior or priorities change.
- For performance optimization, follow the measurement and follow-up guidance
  in `docs/usage/development.md`.
- Work pragmatically and conserve time, compute, and context. Reuse existing
  code and scripts where practical; create focused reusable helpers or bounded
  outputs when they reduce repeated work. Prefer targeted archive queries and
  compact reports, and save verbose corpus-run logs to files. Do not sacrifice
  necessary validation or recall assessment solely to save resources.
- Do not start a full corpus pipeline run without explicit user authorization
  or mutate Anki during these runs. Follow the agent run procedure in
  `docs/usage/running.md`.
- Before a task, check whether a script in `scripts/` can perform it. If none
  exists, strongly consider creating a reusable script. In particular, prefer
  reusable scripts for querying and inspecting output, such as searching for a
  word or other criteria. Avoid unnecessary long-running work and
  avoid lengthy ad hoc code when a single script would suffice.
- Preserve each existing file's line endings when editing. Avoid line-ending-only
  diffs, and check the diff for unintended newline changes before finishing.
- Write and read text files with explicit UTF-8 encoding. On Windows, avoid
  passing non-ASCII file contents through a console with uncertain encoding.
  Read back representative text before testing. If encoding corruption occurs,
  treat the run as invalid, repair the files, and rerun the affected checks.
