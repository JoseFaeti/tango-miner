# 〜につけ〜につけ

- [Meaning](#meaning)
- [Supported behavior and limitations](#supported-behavior-and-limitations)
- [Implementation and evidence](#implementation-and-evidence)

Status: [~]

## Meaning

Whether one alternative or another; in either case.

## Supported behavior and limitations

Two `につけ` markers following nouns, pronouns, adjectives or verbs in the
same sentence produce a match on the first marker, preserving the examples as
vocabulary. Split `に` + `つけ` and single-token `につけ` are supported,
including commas and repeated pairs. Attached auxiliary verbs are excluded.
The split form can also represent the literal verb つける, so ambiguity remains;
other tokenizations and corpus coverage have not been evaluated.

## Implementation and evidence

- Rule: [nitsukenitsuke/index.py](../../../src/grammar_rules/rules/nitsukenitsuke/index.py), identity `nitsukenitsuke`.
- Tests: [test_index.py](../../../src/grammar_rules/rules/nitsukenitsuke/test_index.py), including Sudachi tokenizations, lexical controls, reservation and card identity.
