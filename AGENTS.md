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
- Run `python -m unittest discover -s tests` for pipeline changes and report
  unavailable integration dependencies or test failures. Avoid real Anki
  mutations during tests; inspect the current pipeline before running it.
- Keep documentation free of absolute local paths and personal machine details.
  Preserve the user's existing changes and update relevant documentation when
  behavior or priorities change.
