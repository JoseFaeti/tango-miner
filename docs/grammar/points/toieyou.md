# 〜といえよう

- [Meaning](#meaning)
- [Implementation](#implementation)

Status: [~]

## Meaning

One could say.

## Implementation

Matches adjacent quotative と and the potential verb 言えよう / いえよう,
leaving the preceding proposition available. Sudachi full-dictionary tests cover
both spellings, a following question particle, unrelated potential forms,
token boundaries, reservation and card identity. A literal reading of being
able to say something can overlap; token matching cannot resolve context.
Other spellings and tokenizations remain unverified.

- Rule: [index.py](../../../src/grammar_rules/rules/toieyou/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/toieyou/test_index.py)
- Registry identity: `toieyou`; stable expression: `〜といえよう`.
