# 〜ものではない

Status: [~]

## Meaning

Should not.

## Tracked scope

An adjacent finite or attributive verb followed by `ものではない` expresses
admonition or something improper. The matcher also accepts `ものじゃない`,
`ものではありません`, past negatives, and `ものではないです` when the
negative copula is tokenized in the supported forms. The card span starts at
`もの`. Sudachi full-dictionary tests cover plain, colloquial and polite forms.

The same morphology can describe a literal thing, as in `食べるものではない`;
context is needed to distinguish the meanings. The rule does not itself
reserve lexical tokens, though overlapping rules may do so. Other conjugations, merged analyses and colloquial
`もん` are not covered. Corpus precision and recall have not been evaluated.

## Implementation

- Rule and tests: [monodehanai](../../../src/grammar_rules/rules/monodehanai/)
- Registry identity: `monodehanai`; stable identity: `〜ものではない`.
