# 〜につけても

- [Meaning](#meaning)
- [Supported behavior and limitations](#supported-behavior-and-limitations)
- [Implementation and evidence](#implementation-and-evidence)

Status: [~]

## Meaning

Whatever the occasion; even in relation to.

## Supported behavior and limitations

Recognizes につけても after a noun, pronoun or verb, including split
analyzer tokens. Literal kana つける followed by ても has the same analysis,
so false positives remain possible. Other forms and corpus coverage have not
been evaluated.

## Implementation and evidence

- Rule: [nitsuketemo/index.py](../../../src/grammar_rules/rules/nitsuketemo/index.py), identity `nitsuketemo`.
- Tests: [test_index.py](../../../src/grammar_rules/rules/nitsuketemo/test_index.py), including analyzer forms, controls and card integration.
