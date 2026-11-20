# 〜といえど

- [Meaning](#meaning)
- [Implementation](#implementation)

Status: [~]

## Meaning

Even though; literary.

## Implementation

Matches adjacent と + いえ / 言え + ど, excluding the longer といえども
construction. Sudachi full-dictionary tests cover both spellings, fragments,
spacing, lexical controls, reservation and the longer form. The matcher does
not verify concessive context; other spellings and tokenizations remain unverified.

- Rule: [index.py](../../../src/grammar_rules/rules/toiedo/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/toiedo/test_index.py)
- Registry identity: `toiedo`; stable expression: `〜といえど`.
