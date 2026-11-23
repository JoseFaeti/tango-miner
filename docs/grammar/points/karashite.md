# 〜からして

[Meaning](#meaning) · [Verified behavior and limitations](#verified-behavior-and-limitations)

Status: [~]

## Meaning

Judging from; even.

## Verified behavior and limitations

- Matches a noun or pronoun followed by adjacent Sudachi split-mode C 格助詞
  から, する stem し and 接続助詞 て. The card spans からして and retains
  the preceding noun or pronoun as lexical evidence.
- Rejects complete verb tokens such as 預からし rather than matching an
  internal substring. Sudachi splits the reported あずからしてもらう into
  the apparent name あず plus から・し・て・もらう; a narrow guard rejects
  this sequence when もらう is an adjacent verb. Other analyzer errors remain
  possible; this does not exclude proper nouns generally or repair lexical tokens.
- Regressions retain judgment, emphatic 先生からして, and nominalized
  彼が来たことからして, alongside the reported source and kanji controls.
- Focused tests cover judgment and emphasis, spacing, unrelated forms, card
  identity and token reservation. Dictionary expression precedence may suppress
  the card. The same sequence can express a literal action starting from a
  place or person; the matcher cannot distinguish that reading. Other
  tokenizations and corpus-wide precision and recall remain unmeasured.
