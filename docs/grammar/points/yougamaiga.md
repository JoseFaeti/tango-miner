# 〜ようが〜まいが

- [Meaning](#meaning)
- [Tracked scope](#tracked-scope)
- [Implementation](#implementation)

Status: [~]

## Meaning

Whether or not; regardless of whether an action happens.

## Tracked scope

Recognizes adjacent paired forms with the same verb, including Sudachi's
observed analyses of 行こうが行くまいが, 食べようが食べまいが,
来ようが来まいが and しようがしまいが. The complete pair is reserved
as one grammar span, including both verb occurrences. Focused tests cover
these forms, mismatched verbs, incomplete pairs, spacing and the separate
〜ようと〜まいと construction. Other tokenizations, intervening punctuation,
verb inflections and independent corpus precision and recall remain unassessed.
Dictionary expression precedence can suppress a match.

## Implementation

- Rule and tests: [yougamaiga](../../../src/grammar_rules/rules/yougamaiga/)
- Registry identity: `yougamaiga`; stable identity: `〜ようが〜まいが`.
