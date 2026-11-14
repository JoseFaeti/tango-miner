# 〜とはいえ

Status: [~]

## Meaning

Although.

## Implementation

The rule matches adjacent と + は + いえ / 言え when Sudachi analyzes the
verb as the classical irrealis (已然形), followed by text before the next
sentence boundary. It supports both clause-attached and sentence-initial uses.
The span reserves only とはいえ / とは言え, leaving the preceding words available.
Modern potential forms such as とは言えない are excluded. Tokenization and the
presence of following text do not prove a concessive meaning; other analyses
and variants remain unverified.

- Rule: [index.py](../../../src/grammar_rules/rules/tohaie/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/tohaie/test_index.py)
- Registry identity: `tohaie`; stable expression: `〜とはいえ`.
