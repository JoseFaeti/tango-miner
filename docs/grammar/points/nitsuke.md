# 〜につけ

- [Meaning](#meaning)
- [Supported behavior and limitations](#supported-behavior-and-limitations)
- [Implementation and evidence](#implementation-and-evidence)

Status: [~]

## Meaning

Whenever or on the occasion of.

## Supported behavior and limitations

Recognizes `につけ` after a noun, pronoun or verb, including split analyzer
tokens. Paired alternatives are left to 〜につけ〜につけ; attached auxiliaries
and て continuations are excluded. The split spelling can also be the literal
verb つける, so false positives remain possible. Other forms and corpus
coverage have not been evaluated.

## Implementation and evidence

- Rule: [nitsuke/index.py](../../../src/grammar_rules/rules/nitsuke/index.py), identity `nitsuke`.
- Tests: [test_index.py](../../../src/grammar_rules/rules/nitsuke/test_index.py), including analyzer forms, lexical controls and card integration.
