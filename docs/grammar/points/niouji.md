# 〜に応じ

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)
- [Limitations](#limitations)

Status: [~]

## Meaning

Depending on; in accordance with. Formal connective noun + に応じ.

## Verified coverage

Matches adjacent noun or pronoun + に応じ / におうじ before a continuing
clause, optionally separated by a comma. The preceding noun stays available
as lexical evidence; only に and 応じ are reserved for this grammar identity.

`src/grammar_rules/rules/niouji/test_index.py` checks actual Sudachi full
split-mode C tokens, including 需要に応じ価格を調整する, 状況に応じ、変える,
and kana and pronoun forms. Integration checks cover reservation, example
surface, registry and legacy identity, and the separate
[〜に応じて](nioujite.md) identity. Inflected forms, missing noun context,
whitespace inside the construction and 応えて do not match.

## Limitations

Literal responses such as 呼びかけに応じ立ち上がる also match; syntax alone
does not establish the depending-on sense. Sentence-final fragments are not
supported. Other tokenizations and corpus coverage remain unverified.
[〜に応じた](nioujita.md) is a separate point.
