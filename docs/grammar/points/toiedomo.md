# 〜といえども

- [Meaning](#meaning)
- [Implementation](#implementation)

Status: [~]

## Meaning

Even; although. This formal concessive construction presents an exception
or unexpected outcome despite the preceding situation or identity.

## Implementation

Matches adjacent と + いえ / 言え + ど + も, reserving only the construction.
Sudachi full-dictionary tests cover both spellings, the shorter といえど,
spacing, lexical controls and reservation. It also retains sentence-final
fragments; the matcher does not determine whether the context is concessive.
Other spellings and tokenizations remain unverified.

- Rule: [index.py](../../../src/grammar_rules/rules/toiedomo/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/toiedomo/test_index.py)
- Registry identity: `toiedomo`; stable expression: `〜といえども`.
