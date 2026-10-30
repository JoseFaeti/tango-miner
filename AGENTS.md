# Working on Tango Miner

Read `README.md` and `docs/false-positive-analysis.md` before changing mining
behavior; consult `docs/pipeline-improvement-roadmap.md` for broader context.
The current code and tests determine actual behavior when a document is stale.

- Preserve the project's recall-first goal: useful vocabulary, grammar,
  expressions, colloquialisms, names, and uncertain candidates are in scope.
  Do not turn suspended examples into a blacklist or introduce broad filters
  based on kana, POS, length, or frequency without assessing recall loss.
- When improving a target or definition, ground it in the source and retain
  relevant component evidence. Test both corrected false positives and useful
  candidates that must remain. Do not assume stored provenance is already
  used for selection or exported.
- Existing behavior does not always meet the intended policy: standalone
  proper nouns are currently filtered. Identify such gaps explicitly and add
  focused regressions when changing them.
- Run `python -m unittest discover -s tests` for pipeline changes. A passing
  full suite is required: investigate and fix failures rather than accepting
  them as a baseline. Preserve meaningful regression coverage and report any
  unavailable integration dependencies. Avoid real Anki mutations during
  tests; inspect the current pipeline before running it.
- Keep documentation free of absolute local paths and personal machine details.
  Preserve the user's existing changes and update relevant documentation when
  behavior or priorities change.
- Work pragmatically and conserve time, compute, and context. Reuse existing
  code and scripts where practical; create focused reusable helpers or bounded
  outputs when they reduce repeated work. Prefer targeted archive queries and
  compact reports, and save verbose corpus-run logs to files. Do not sacrifice
  necessary validation or recall assessment solely to save resources.
- Full pipeline runs require a curated input corpus agreed with the user in
  advance; source files may be outside the project directory. Before starting,
  verify the resolved configuration has Anki disabled or in output-only mode.
  Save verbose output to a log and wait for the process completion signal or
  exit status before inspecting the run, its log, or its artifacts. Then check
  the result and report failures. Do not mutate Anki during these runs.
- Before a task, check whether a script in `scripts/` can perform it. If none
  exists, strongly consider creating a reusable script. In particular, prefer
  reusable scripts for querying and inspecting output, such as searching for a
  word or other criteria. Avoid unnecessary long-running work and
  avoid lengthy ad hoc code when a single script would suffice.
- Preserve each existing file's line endings when editing. Avoid line-ending-only
  diffs, and check the diff for unintended newline changes before finishing.
