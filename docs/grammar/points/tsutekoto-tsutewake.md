# 〜ってこと / 〜ってわけ

Status: [~]

## Meaning

Colloquial explanation or inference: “that means” or “that is why.”

## Verified coverage

The rule recognizes predicate-final ってこと and ってわけ, including 事 and
訳, with だ, です, だった, or でした, and bare endings. The predicate must
be an adjacent finite verb, adjective, or auxiliary. The matched span starts
at って; the preceding predicate remains available as lexical evidence.
The longer expression takes precedence over the standalone って rule.
Sudachi full-dictionary examples and reservation, identity, spacing and
lexical controls are covered by `ttekoto_ttewake/test_index.py`.

## Remaining limitations

Noun and adjectival-noun predicates, negative and other inflections, and
embedded uses such as ってことは are not yet covered. Bare endings can be
ambiguous with nominal uses; the rule accepts them only before punctuation or
at the end of the token stream. The separate 〜わけ / 〜ってわけ point
handles bare non-contracted explanatory わけ.
