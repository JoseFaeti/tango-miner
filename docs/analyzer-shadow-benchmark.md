# Offline analyzer comparison

This shadow benchmark compares raw-source token spans from Sudachi and a
UniDic-backed fugashi/MeCab analyzer. It does not modify mining behavior,
send source text elsewhere, contact Anki, or claim analyzer disagreement is an
error. These analyzers have different word-unit and POS conventions.

## Raw source sample

```sh
python -X utf8 scripts/compare_japanese_analyzers.py sample --run runs/example-run --output checks/raw.jsonl
python -X utf8 scripts/compare_japanese_analyzers.py analyze --sample checks/raw.jsonl --analyzer sudachi-c --output checks/sudachi.jsonl
python -X utf8 scripts/compare_japanese_analyzers.py analyze --sample checks/raw.jsonl --analyzer unidic --output checks/unidic.jsonl
python -X utf8 scripts/compare_japanese_analyzers.py compare --sample checks/raw.jsonl --left checks/sudachi.jsonl --right checks/unidic.jsonl
python -X utf8 scripts/compare_japanese_analyzers.py label-template --sample checks/raw.jsonl --output checks/labels.jsonl
```

Use a real completed run path. Run from the project root so the Sudachi config's
relative user-dictionary paths are interpreted consistently. Create the output
directory first; existing output files are never overwritten. The source sample
uses the archived input manifest, verifies selected file hashes, and samples up
to three Japanese-containing raw lines from each of 24 deterministically chosen
source-tag groups. A group is assigned wholly to development or holdout.
`--seed`, `--max-groups`, and `--per-group` change this selection. This samples
groups first, not individual sentences uniformly; it can include short lines
and lines that the normal pipeline would exclude. Lines over 300 characters
are excluded. A source group can have fewer than the requested number of
eligible lines or none. Tag grouping is a heuristic: untagged files or different
tags from the same work may still leak across partitions, and duplicates are
not removed. Audit grouping and deduplicate before treating holdout as an
independent test.

Rows contain raw line text, a source file content hash, original line number,
partition and group ID. They deliberately omit absolute source paths. Analyzer
spans are zero-based, end-exclusive Python Unicode character offsets into that
raw line; each nonempty token surface is checked against the original text in
order, allowing only whitespace gaps. Empty Sudachi nodes are omitted. Failures
are recorded as errors, not guessed offsets. Because this analyzes raw lines,
it is not the current pipeline's extracted, normalized and merged output.
Tabs, script markup and multiple sentences may appear on a line. Future
evaluation must also align pipeline-normalized output back to original text.
The sample and analysis files contain source text and should be treated as
private. Package versions and dictionary/configuration hashes accompany analysis.

Each label template entry starts `reviewed: false` with an empty `annotations`
list. A reviewed annotation should identify a source `[start, end]` span,
surface, intended learning target and type, reading if known, acceptable
dictionary entry/sense if available, and alternatives or uncertainty. The
template is deliberately not auto-labeled. Current `compare` reports exact
span-set agreement and analysis errors; it does not score the labels, reading,
POS, target, meaning, recall or correctness. A gold-aware scorer and an
independently adjudicated annotation protocol remain necessary.

## Candidate scoring pilot

`scripts/score_sentence_candidates.py` scores source-aligned learning candidate
proposals, not analyzer tokens. It accepts the existing label template with
two additional fields: set `exhaustive: true` only after reviewing the whole
line for the agreed scope of useful targets, and put each intended candidate
in `annotations` as an object with an `alternatives` list. Each alternative
has `span` (zero-based, end-exclusive), exact `surface`, and nonempty `target`.
Alternatives represent interchangeable answers for one candidate; put distinct
overlapping learning candidates in separate annotation objects. An empty list
of annotations means no candidates only when the line is reviewed and exhaustive.
Record uncertainty in the note; do not mark a line exhaustive when unresolved
uncertainty prevents a complete judgment. Agree on annotation scope and adjudicate
independently before using these labels as gold.

Predictions are separate JSONL rows with the same IDs and a `candidates` list
using the same span/surface/target fields, or `abstain: true` or a nonempty
`error`. Do not expose labels to the system being evaluated. For example:

```sh
python -X utf8 scripts/score_sentence_candidates.py --sample checks/raw.jsonl --labels checks/labels.jsonl --predictions checks/predictions.jsonl
```

The report counts matched, proposed, and gold candidates separately by partition,
using one-to-one matching across acceptable alternatives. Precision is matched
divided by proposed and recall is matched divided by gold, when denominators are
nonzero. Unreviewed, non-exhaustive, abstained and error lines are counted but
excluded from those denominators: report coverage alongside scores. This is an
exact span-and-target pilot; it does not score readings, POS, senses, partial
overlaps, or correctness on abstained lines. A system that abstains selectively
must be compared at matched coverage. The audit flags exact duplicate text and
group IDs crossing partitions, but does not find near duplicates or related
works under different tags. Resolve those leaks manually before using holdout.
Raw-line samples may contain markup or multiple sentences and are not uniformly
sampled corpus sentences; results are not corpus-wide estimates.

## Initial observation (2026-10-28)

A local sample of 69 lines from 23 nonempty groups produced no alignment errors
after ignoring empty Sudachi nodes. Sudachi C and UniDic had different span
sets on 43/57 development lines and 7/12 holdout lines. These are raw-line
disagreements under different segmentation standards, not quality scores.

A separate hand-selected development set of six source-like examples shows:

- UniDic splits `こんなにも` around the fragment `なにも`, while Sudachi
  treats the latter as a noun token.
- Both analyzers label negative `ないし` as a conjunction in the chosen
  sentence, and both also find the genuine conjunction in an alternative-use
  example.
- UniDic separates `もの / の` in both a noun phrase and a valid concessive
  `ものの`; Sudachi emits the conjunction in both. Neither segmentation alone
  distinguishes their meanings.
- Both produce a `ごく` token inside `ごくもん`, with different POS tags.

The `focus-report` subcommand summarizes whether an annotated focus span is
exactly one analyzer token. In this six-item set, Sudachi emitted an exact token
for all four unsupported focuses and both supported focuses; UniDic emitted an
exact token for two of four unsupported focuses and one of two supported
focuses. This is an illustration of the deliberately naive exact-token
heuristic, not a measurement of either analyzer's accuracy or a proposal to
exclude multi-token expressions. For example, a split valid `もの / の` can
still support the complete expression.

These examples are diagnostic and were selected because they are hard; they
are neither an accuracy sample nor independent holdout data. None of the 69
raw lines has been independently annotated yet. KWJA and GiNZA were not
installed or tested. Next, annotate and adjudicate source-aligned spans and
target interpretations, implement gold-aware scoring, and test contextual
analysis on a held-out, leakage-audited set before changing mining behavior.
