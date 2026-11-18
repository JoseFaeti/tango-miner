# 〜いかんで / 〜いかんによっては

- [Meaning](#meaning)
- [Verified behavior](#verified-behavior)

Status: [~]

## Meaning

Depending on the circumstances or outcome of the preceding noun.

## Verified behavior

- After a noun or pronoun, optionally followed by の, the rule recognizes
  いかん／如何 followed by で or によって, with optional は after によって.
  では belongs to the separate 如何では rule. It reserves
  only the grammar expression, leaving the attached noun available for mining.
  It also handles Sudachi's combined adverb token 如何で.
- Focused tests cover split-mode C forms, source boundaries, unrelated readings,
  card identity and token reservation. The rule requires adjacent complete
  tokens. Other analyzer segmentations and contextual ambiguities remain
  unverified; the surface pattern cannot prove the intended reading of 如何.
