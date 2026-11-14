# 〜といっても

Status: [~]

## Meaning

Although one says; even so. The following clause qualifies or limits the
expectation raised by the preceding description.

## Implementation

Matches adjacent と + いっ / 言っ + て + も, with following text before a
sentence boundary, including sentence-initial usage. Only the construction is
reserved, leaving preceding words available. Sudachi full-dictionary tests
cover both spellings, lexical 行く, incomplete phrases and spaced tokens.
Literal quoted speech can share this token sequence, so meaning is ambiguous;
colloquial variants and other tokenizations remain unverified.

- Rule: [index.py](../../../src/grammar_rules/rules/toitsutemo/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/toitsutemo/test_index.py)
- Registry identity: `toitsutemo`; stable expression: `〜といっても`.
