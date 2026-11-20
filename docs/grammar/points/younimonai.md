# 〜ようにも〜ない

- [Meaning](#meaning)
- [Coverage](#coverage)
- [Implementation](#implementation)

Status: [~]

## Meaning

Even if one tries, cannot.

## Coverage

Matches an adjacent volitional verb, にも and a negative verb, including
potential auxiliaries, plain past and polite negatives. Sudachi tests cover
行こうにも行けない, 食べようにも食べられない, しようにもできない
and 来ようにも来られなかった. The entire construction is reserved.
The verbs need not match, so unrelated negative predicates can be captured;
meaning and ability cannot be established by morphology alone. Intervening
words, other inflections and tokenizations remain unverified. Dictionary
expression precedence may suppress a match.

## Implementation

- Rule and tests: [younimonai](../../../src/grammar_rules/rules/younimonai/)
- Registry identity: `younimonai`; stable identity: `〜ようにも〜ない`.
