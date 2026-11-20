# 〜とはいっても

[Contents](#meaning) · [Implementation](#implementation)

Status: [~]

## Meaning

Even so; that said. A following clause qualifies an earlier statement or
expectation.

## Implementation

Matches adjacent と + は + いっ / 言っ + て + も with following text before a
sentence boundary, including sentence-initial usage. Only the construction is
reserved; preceding words remain available. Sudachi full-dictionary tests
cover both spellings, lexical 行く, incomplete phrases, spacing, and the
separate 〜といっても form. Literal quoted speech can share this sequence;
other tokenizations and colloquial variants remain unverified.

- Rule: [index.py](../../../src/grammar_rules/rules/tohaitsutemo/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/tohaitsutemo/test_index.py)
- Registry identity: `tohaitsutemo`; stable expression: `〜とはいっても`.
