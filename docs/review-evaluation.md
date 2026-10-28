# Review evaluation

This offline workflow samples selected examples from a completed run. It does
not run mining, contact Anki, or automatically judge any candidate. Use it to
compare the current output and experimental disambiguation against independently
reviewed judgments, rather than treating a smaller review queue as success.

```sh
python -X utf8 scripts/evaluate_review.py sample --output checks/sample.jsonl
python -X utf8 scripts/evaluate_review.py label-template --sample checks/sample.jsonl --output checks/labels.jsonl
python -X utf8 scripts/evaluate_review.py prompts --sample checks/sample.jsonl --output checks/prompts.jsonl
python -X utf8 scripts/evaluate_review.py score --sample checks/sample.jsonl --labels checks/labels.jsonl
python -X utf8 scripts/evaluate_review.py score --sample checks/sample.jsonl --labels checks/labels.jsonl --predictions checks/predictions.jsonl
```

Create the output directory first. The commands refuse to overwrite files.
By default the sample has 20 candidates each from high, middle, and low
frequency thirds of valid and dropped/review output, using a fixed seed.
Supply `--run`, `--seed`, or `--per-stratum` to change the selection. A
candidate without a selected sentence cannot be sampled; the command reports
how many are excluded. The file records the run and dictionary hashes. By
default a dictionary whose hash differs from the archive is rejected; an
explicit override is available for investigations where the original is lost.
The CSV conflates invalid and unresolved candidates, so those two groups cannot
be separated from this archive alone.

Each row contains one pipeline-selected example, not a randomly sampled corpus
occurrence. It does not reliably identify the character span, particularly
when the surface occurs multiple times. The dictionary alternatives are exact
lookup matches, not an exhaustive set of alternative boundaries or inflected
forms. They are unfiltered raw JMdict senses, so check spelling and reading
restrictions. Sampling is balanced by stratum, not by corpus prevalence, and
cannot by itself estimate occurrence-weighted accuracy, recall, or errors in
missing candidates. Sampled text and tags may contain private source material;
keep it local unless sharing is deliberate.

## Annotation

For each selected example, inspect the source and alternatives before filling
the JSONL label template. `baseline_correct` is `correct` or `incorrect` only
when the pipeline definition and selected example agree; use `uncertain` when
the evidence is insufficient and `not_applicable` for an unresolved baseline.
Leave the value empty if not reviewed. Record why in `review_note`. For
independent predictions, `gold_target` is the intended candidate and
`gold_entry_id` is its JMdict `ent_seq`; leave either empty when they cannot be
established. Do not guess an entry for a valid candidate absent from JMdict.
Gold fields are not required for the baseline judgment. If multiple meanings
are defensible, or distinct senses share an entry, the simple single-entry
scorer is insufficient: leave the gold fields empty and explain the ambiguity.
Review the labels independently before treating preliminary judgments as gold.

The `prompts` command exports only the candidate, sentence, surface and
dictionary alternatives; it omits labels, baseline definition and output
status. It does not call or select a model. A separate offline experiment may
write one prediction per ID as either
`{"id":"…","decision":"abstain"}` or
`{"id":"…","decision":"select","target":"…","entry_id":"…"}`.
Do not expose the labels to the evaluated method. A model must be allowed to
abstain rather than forced to choose an offered entry, since the correct target
may be absent. Keep its configuration and version with its output.

The scoring report shows counts by stratum and overall. Baseline judgments are
reported separately from model predictions. Predictions are scored only for
rows with both gold fields; missing predictions count as abstentions, and
unlabeled cases are never counted as correct. Matching an entry ID does not
verify the chosen sense or target span. This is a first comparison harness,
not a complete measure of recall, confidence calibration, or example quality.
Report the number labeled, selection coverage, errors, abstentions and sampling
limitations alongside any result; do not interpret a small pilot as a corpus
estimate.
