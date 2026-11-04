# 〜ないと / 〜なくちゃ / 〜なきゃ

Status: [~]

## Meaning

Obligation.

This entry concerns standalone elliptical obligations. The related
[complete obligation rule](obligation.md) already supports these forms when
followed by selected complete endings such as `いけない` or `ならない`.

## Implementation and limits

The `elliptical_obligation` rule matches the three suffixes after an adjacent
verb in a negative-compatible form, at the end of an utterance or before
selected final particles and punctuation. It reserves the suffix, leaving the
verb available as lexical evidence. Conditional clauses followed by another
word and complete obligations are excluded. Sentence-final conditionals can
still be ambiguous; merged verb-plus-suffix tokens, other punctuation and
non-final elliptical uses are not covered. Independent precision and recall
have not been measured.

The implementation is in [elliptical_obligation.py](../../../src/grammar_rules/rules/elliptical_obligation.py),
with focused cases in [test_grammar_elliptical_obligation.py](../../../src/grammar_rules/rules/tests/test_grammar_elliptical_obligation.py).
These cases include analyzer tokenizations, controls, reservation and card
identity; they have not been run for this change.
