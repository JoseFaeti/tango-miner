# 〜かけだ

Status: [~]

## Meaning

Unfinished or in progress.

## Implementation

- Rule: [kakeda](../../../src/grammar_rules/rules/kakeda/index.py)
- Tests: [kakeda tests](../../../src/grammar_rules/rules/kakeda/test_index.py)
- Registry identity: `kakeda`; stable expression: `〜かけだ`.

Matches a merged nominal verb stem ending in かけ or 掛け followed by だ,
だった, です or でした. The span includes the stem; lexical candidates are
retained. Sudachi full dictionary produces merged nouns for 読みかけ and
書き掛け. A hiragana character immediately before the suffix is required to
avoid nouns such as 仕掛け, but this surface heuristic cannot establish the
meaning: words such as きっかけ can still match and need review. Separate
tokenizations, including Sudachi's 死にかけだ analysis, other copular forms,
remain unsupported. Attributive 〜かけの is handled by the
[〜かける rule](kakeru.md).
