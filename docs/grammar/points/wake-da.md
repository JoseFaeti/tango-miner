# 〜わけだ / 〜わけではない

Status: [~]

## Meaning and tracked scope

The explanatory card describes a conclusion or reason; the separate negative
card describes a denial of an inference or partial negation. Predicate-attached
わけ and 訳 are supported with finite だ / です and だった / でした, and
では / じゃ followed by ない, なかった, ありません or ありませんでした.
The plain negative forms also accept following です.

## Implementation and evidence

- Rules: [wake_da.py](../../../src/grammar_rules/rules/wake_da/index.py)
- Tests: [test_grammar_wake_da.py](../../../src/grammar_rules/rules/wake_da/test_index.py)
- Registry identities: `wake_da` and `wake_dewa_nai`.
- Stable card identities: `〜わけだ` and `〜わけではない`.

Tests exercise source-aligned Sudachi full-dictionary tokenization, formal and
colloquial negatives, kanji spelling, incomplete suffixes and nominal controls.
A reservation and card example test checks that the attached predicate remains
available. Grammar spans reserve the construction subject to the shared
[dictionary precedence](../stage-and-precedence.md) and example rules.

The predicate cue does not establish meaning: a predicate can modify a literal
reason or translation, and negative uses are not always partial negation.
Noun + の, bare わけ, continuative and other inflected forms remain unsupported.
Dictionary precedence, corpus-wide precision and recall, and other tokenizations
have not been independently evaluated. Standalone proper nouns remain filtered
elsewhere in the pipeline.
