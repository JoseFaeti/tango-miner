# 〜にくい / 〜づらい / 〜がたい

Status: [~]

## Meaning

Difficulty doing something. にくい commonly describes difficulty in performing
an action; づらい can emphasize discomfort or subjective difficulty; がたい
often expresses that an action is hard to accept or almost impossible. These
are tendencies, and the matcher does not infer the nuance from context.

## Implementation

- Rule: [nikui_zurai_gatai](../../../src/grammar_rules/rules/nikui_zurai_gatai/index.py)
- Tests: [difficulty tests](../../../src/grammar_rules/rules/nikui_zurai_gatai/test_index.py)
- Registry identity: `nikui_zurai_gatai`; stable expression: `〜にくい / 〜づらい / 〜がたい`.

A source-aligned continuative verb followed by an adjectival suffix with lemma
`にくい`, `難い`, `づらい`, `辛い` or `がたい` matches from the suffix.
Plain, past, negative, conditional, continuative, te and plain plus `です`
forms use the shared adjective inflection matcher. Sudachi full dictionary
tests cover the three kana suffixes, `行き難い`, unrelated adjectives, token
boundaries and source examples. The `〜やすい / 〜にくい` card also receives
matching にくい and 難い occurrences; the verb remains available as lexical
evidence. Morphology cannot establish the intended nuance or distinguish every
lexicalized compound. Merged verb-suffix tokens, kanji `辛い` as a suffix, and
other spellings remain unverified.
