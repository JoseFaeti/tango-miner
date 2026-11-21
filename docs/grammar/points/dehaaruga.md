# 〜ではあるが

- [Meaning](#meaning)
- [Tracked scope](#tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning

Concedes a description before a contrasting statement: “although” or
“admittedly ... but”.

## Tracked scope

Recognizes adjacent `ではあるが` and `じゃあるが` after a nominal,
pronoun, adjectival noun, or auxiliary. The preceding token remains lexical
evidence. Tests cover Sudachi forms, incomplete and spaced controls, registry,
reservation and examples. A locative existence reading such as `学校ではあるが`
is surface-ambiguous and can also match. Other forms, segmentations, and corpus
precision and recall remain unassessed.

## Implementation

- Rule and tests: [dehaaruga](../../../src/grammar_rules/rules/dehaaruga/)
- Registry identity: `dehaaruga`; stable identity: `〜ではあるが`.
