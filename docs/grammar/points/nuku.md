# 〜抜く

Status: [~]

## Meaning

Do something thoroughly or persist in doing it to the end.

## Implementation

- Rule: [nuku](../../../src/grammar_rules/rules/nuku/index.py)
- Tests: [nuku tests](../../../src/grammar_rules/rules/nuku/test_index.py)
- Registry identity: `nuku`; stable expression: `〜抜く`.

Matches an adjacent continuative verb and 抜く / ぬく, or a merged
kanji-spelled compound. Supported forms include plain, past, te, polite,
negative and conditional. The span includes the preceding verb and leaves
tokens available as lexical evidence. Standalone 抜く is excluded.

Sudachi full dictionary examples include 走り抜いた, 生き抜いて and
考え抜きます. Compounds may have literal or lexicalized meanings and need
review. Merged kana compounds, potential forms, other inflections and
analyzer segmentations remain unsupported.
