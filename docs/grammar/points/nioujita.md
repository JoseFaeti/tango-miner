# 〜に応じた

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)
- [Limitations](#limitations)

Status: [~]

## Meaning

Suited to; in accordance with. Attributive noun + に応じた + noun.

## Verified coverage

Matches adjacent noun or pronoun + に応じた / におうじた before a noun.
Only に応じた is reserved; both surrounding nouns remain lexical evidence.
The registry and legacy identity are derived from the rule declaration.

`src/grammar_rules/rules/nioujita/test_index.py` checks Sudachi full split-mode C
forms including 能力に応じた仕事, 状況に応じた対応, kana and pronoun forms,
and reservation, example surface and separate identities from
[〜に応じ](niouji.md) and [〜に応じて](nioujite.md).
Controls reject sentence-final past responses, other inflections, missing noun
context, internal whitespace, punctuation before the head noun and 応えた.

## Limitations

Literal responses such as 呼びかけに応じた人 also match: syntax alone cannot
separate them from the suited-to sense. Other tokenizations and corpus coverage
remain unverified; this is partial coverage rather than a semantic classifier.
