# 〜ではあるまいか

Status: [~]

## Meaning

Tentative supposition or a rhetorical question: “might it not be” or “perhaps”.

## Tracked scope

Recognizes adjacent `ではあるまいか` and `じゃあるまいか` after a nominal,
pronoun, adjectival noun, or auxiliary, including quoted questions. The card
starts at `では` or `じゃ`, retaining the preceding token as lexical evidence.
Tests cover Sudachi tokenization, incomplete and spaced forms, reservation and
examples. Sudachi can analyze `で` as a case particle, so a locative reading
such as “might it not exist at school?” is indistinguishable from the copula
on the surface and may also match. Other segmentations and corpus precision and
recall remain unassessed.

## Implementation

- Rule and tests: [dehaarumaika](../../../src/grammar_rules/rules/dehaarumaika/)
- Registry identity: `dehaarumaika`; stable identity: `〜ではあるまいか`.
