# 〜に関しては

- [Meaning and forms](#meaning-and-forms)
- [Implementation and verification](#implementation-and-verification)
- [Limitations](#limitations)

Status: [~]

## Meaning and forms

Noun + に関しては introduces a topic, often contrastively: “as for;
concerning; regarding”. The stable card is 〜に関しては.

## Implementation and verification

`src/grammar_rules/rules/nikanshiteha/` matches adjacent に + 関し + ては
or separate て + は. Sudachi full split-mode C produces the joined tail in
契約に関しては、後日説明します。 and the split tail in
それに関しては問題ありません。 The preceding noun, pronoun or nominal
suffix remains available as vocabulary; only に関しては is reserved.

Tests cover both tokenizations, nominal attachment, missing は, related
constructions, punctuation and alignment gaps, card frequency and identity,
example surfaces and reservation. The broader 〜に関して / 〜に関する rule
still independently recognizes に関して when て is separate, preserving
its existing card alongside this one.

## Limitations

Polite に関しましては, attributive に関する and に関しての are outside
this card. Kana-only spellings and other analyzer tokenizations remain
unverified, so this point is partial. No corpus precision claim is made.
