# Next steps

A short, prioritized working list. Keep useful vocabulary, grammar, names,
colloquialisms, and uncertain candidates in scope. See the
[roadmap](pipeline-improvement-roadmap.md) for context and the
[issue inventory](problematic-entries.md) for example frequencies.

- [x] Use token evidence for contextual dictionary selection across
  homophones, with `する`, `いる`, and `ようやく` as regressions. Match
  examples to the selected analysis and report mixed occurrence counts.
- [ ] Review the 995 net additional unresolved candidates in the full-corpus
  check; improve coverage without guessing same-POS homophones. Present or
  separately review alternative supported meanings and merged analyses.
- [ ] Resolve verb and inflection mismatches such as `おって`, `覆われ`, and
  `つれて` using source and POS evidence; preserve useful expression uses.
- [ ] Separate expression and verb occurrences, starting with `にかけて`;
  preserve both the swearing expression and ordinary `掛ける` uses.
- [ ] Address `により` and investigate remaining boundary errors such as
  `ないし` and `つかる` without discarding valid occurrences.
- [ ] Recheck the archived `しれる`, `ことし`, `わる`, and `疲れた` examples
  against current code; distinguish fixed cases from remaining mixed contexts.
- [ ] Investigate standalone proper-noun filtering and improve name handling;
  add recall regressions before changing eligibility.
- [ ] Improve occurrence diagnostics to capture rejected merges and selected
  dictionary entries; make mixed analyses and unresolved candidates easier to
  review. See [false-positive analysis](false-positive-analysis.md).
- [ ] Improve Anki safety and planning: separately opt in to pruning and track
  applied operations or partial failure. The detailed plan already exists; see
  the [roadmap](pipeline-improvement-roadmap.md).
