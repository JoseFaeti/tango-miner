# 〜もの / 〜もん

Status: [~]

## Meaning

Conversational reason.

## Tracked scope

Matches adjacent terminal or attributive verbs, adjectives, or auxiliaries
followed by nominal or sentence-final particle `もの` or `もん` at a sentence or quotation boundary. The
span contains only `もの` or `もん`; component tokens remain available. This
includes na-adjective and noun predicates with attributive `な`. Literal
nominal readings such as `買ったもの。` are indistinguishable and remain review
candidates. Longer forms such as `ものだから`, `ものか`, `ものだ`, and
`もんね` are excluded. Sentence-final particles, merged tokenizations, and
corpus precision and recall remain unverified.

## Implementation

- Rule and tests: [mono_mon](../../../src/grammar_rules/rules/mono_mon/)
- Registry identity: `mono_mon`; stable identity: `〜もの / 〜もん`.
