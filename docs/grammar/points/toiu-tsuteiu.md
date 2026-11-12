# 〜という / 〜っていう

Status: [~]

## Meaning

Quotation, name or explanation.

## Verified coverage

Matches attributive `という`, `と言う`, `っていう` and `って言う` between
an adjacent preceding token and a following noun, for example 猫という動物,
東京という町 and 行くという話. The grammar span reserves the particle and
verb, leaving the named or quoted material and following noun available as
lexical evidence. Dictionary-expression precedence still applies.

Focused tests exercise Sudachi tokenization, both spellings, naming and
quotation, source spans, reservation, and unrelated or nonadjacent forms.
Attributive literal speech (such as a person who says something) is ambiguous
and can also match. Other inflections, sentence-final quotation, nominalized
and longer constructions, and quoted material ending in punctuation are not
specifically supported. The longer 〜ということだ rule takes precedence when it
matches; other longer constructions may still contain this shorter match.
