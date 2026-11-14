# 〜通す

Status: [~]

## Meaning

Persist in doing something through to the end.

## Implementation

- Rule: [toosu](../../../src/grammar_rules/rules/toosu/index.py)
- Tests: [toosu tests](../../../src/grammar_rules/rules/toosu/test_index.py)
- Registry identity: `toosu`; stable expression: `〜通す`.

Matches an adjacent continuative verb and 通す / とおす, or a merged
kanji-spelled compound. Supported forms include plain, past, te, polite,
negative and conditional. The span includes the preceding verb and leaves
tokens available as lexical evidence. Standalone 通す is excluded.

Compounds may have literal or lexicalized meanings and need review. Merged
kana compounds, potential forms, other inflections and analyzer segmentations
remain unsupported.
