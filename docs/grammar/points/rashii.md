# 〜らしい

Status: [~]

## Meaning

Hearsay or characteristic.

## Implementation

- Rule: [rashii](../../../src/grammar_rules/rules/rashii/index.py)
- Tests: [rashii tests](../../../src/grammar_rules/rules/rashii/test_index.py)
- Registry identity: `rashii`; stable expression: `〜らしい`.

Matches adjacent Sudachi auxiliary or adjective-like suffix forms after nouns,
pronouns, adjectival nouns, and terminal or attributive predicates. Plain, past,
negative, te, adverbial and conditional forms and optional `です` are supported.
The span starts at `らしい`, retaining the attached word; ordinary dictionary
precedence and reservation apply. Tests cover Sudachi forms, lexical adjectives,
incomplete forms, card identity and reservation.

Morphology does not distinguish hearsay from characteristic uses. Standalone or
other segmentations and attachments are unsupported, including lexicalized
adjectives such as `素晴らしい`. Corpus precision and recall are unmeasured.
See [stage and precedence](../stage-and-precedence.md).
