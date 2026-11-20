# 〜とも限らない

- [Meaning](#meaning)
- [Implementation](#implementation)

Status: [~]

## Meaning

Not necessarily; something is not always true.

## Implementation

Matches adjacent とも + 限る / かぎる in negative plain, past, selected
continuative and conditional forms, and polite negative forms. The preceding
predicate remains available. Adjacent negative ない is handled by the distinct
〜ないとも限らない rule. Tests cover Sudachi tokenization, inflections,
spacing, positive 限る, reservation and card identity. Context can still make
the reading ambiguous; literary ぬ and other variants remain unverified.

- Rule: [index.py](../../../src/grammar_rules/rules/tomokagiranai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/tomokagiranai/test_index.py)
- Registry identity: `tomokagiranai`; stable expression: `〜とも限らない`.
