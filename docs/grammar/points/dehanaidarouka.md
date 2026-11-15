# 〜ではないだろうか

Status: [~]

## Meaning

Tentative suggestion.

## Verified coverage

Matches adjacent `ではないだろうか` after nominal and plain predicates,
including explanatory `の`. The example reserves the suffix while retaining
the preceding lexical evidence. Focused tests cover tokenization, embedded
questions, controls, and overlap with the shorter conjecture card. The grammar
stage can retain overlapping cards; ordering does not suppress them.

## Limitations

Literal negative questions share this form, so intent is not distinguished.
Colloquial `じゃないだろうか`, polite `ではないでしょうか`, past forms,
other segmentations, and corpus precision and recall remain unassessed.

## Implementation

- Rule and tests: [dehanaidarouka](../../../src/grammar_rules/rules/dehanaidarouka/)
- Registry identity: `dehanaidarouka`; stable identity: `〜ではないだろうか`.
