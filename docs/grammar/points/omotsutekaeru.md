# 〜をもって代える

- [Meaning](#meaning)
- [Coverage](#coverage)

Status: [~]

## Meaning

Use one thing in place of another, often in formal expressions such as using
written words in place of a greeting.

## Coverage

Matches noun + をもって / を持って / を以て / をもちまして, optionally followed by a short
noun phrase and に, and 代える in plain, past, polite, te, conditional, or
代えさせて forms. The example span includes the intervening phrase; the rule
does not reserve it or 代える from lexical mining. The overlapping をもって
rule can still reserve its own tokens. Sudachi-backed tests cover spelling,
inflection, spacing and unrelated verbs.

This is partial: longer or more complex intervening phrases, kana spellings of
代える, negative forms and other inflections are not covered. Surface matching
cannot distinguish every literal substitution from the formal construction.
