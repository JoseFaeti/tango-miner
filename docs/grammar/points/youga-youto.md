# 〜ようが / 〜ようと

- [Meaning](#meaning)
- [Tracked scope](#tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning

Even if; regardless of whether an action occurs.

## Tracked scope

Recognizes observed volitional verb forms followed directly by が or と,
including 降ろうが, 食べようと, 来ようが and しようと. Complete paired
〜ようが〜まいが and 〜ようと〜まいと matches are left to their own rules.
The common intention and attempt constructions 〜ようと思う and 〜ようとする
are excluded. Other quotations with と and some uses of が remain ambiguous;
the rule can match them without establishing a concessive meaning. Other
tokenizations, inflections, dictionary precedence, and independent corpus
precision and recall remain unassessed.

## Implementation

- Rule and focused tests: [youga_youto](../../../src/grammar_rules/rules/youga_youto/)
- Registry identity: `youga_youto`; stable identity: `〜ようが / 〜ようと`.
