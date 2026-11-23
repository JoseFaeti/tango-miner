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
are excluded. Tests cover single-token and split volitionals, adjacency, and
やくそう followed by と. Sudachi analyzes that noun as volitional やくす in
the reported unpunctuated sentence; the rule suppresses this analysis after
なかれ when と is followed immediately by a noun. This narrow context can also
hide a genuine volitional reading. Other quotations with と and some uses of が
remain ambiguous; the rule can match them without establishing a concessive meaning. Other
tokenizations, inflections, dictionary precedence, and independent corpus
precision and recall remain unassessed.

## Implementation

- Rule and focused tests: [youga_youto](../../../src/grammar_rules/rules/youga_youto/)
- Registry identity: `youga_youto`; stable identity: `〜ようが / 〜ようと`.
