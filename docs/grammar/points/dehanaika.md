# 〜ではないか

Status: [~]

## Meaning

Appeal, emphasis, or realization: “isn't it” or “surely”.

## Verified coverage

The rule recognizes adjacent `ではないか` and `ではありませんか` after
nominal and plain predicates, including explanatory `の`. The example covers
the suffix and leaves the preceding lexical evidence available. Focused tests
cover source examples, reservation, literal and embedded questions, and the
separate volitional invitation and colloquial `じゃないか` rules.

## Limitations

Literal negative questions and embedded questions have the same surface form
as appeal or realization; the matcher does not determine intent. Other
segmentations, variants, and corpus precision and recall remain unassessed.

## Implementation

- Rule and tests: [dehanaika](../../../src/grammar_rules/rules/dehanaika/)
- Registry identity: `dehanaika`; stable identity: `〜ではないか`.
