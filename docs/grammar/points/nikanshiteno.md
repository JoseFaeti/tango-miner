# 〜に関しての

- [Meaning and forms](#meaning-and-forms)
- [Implementation and verification](#implementation-and-verification)
- [Limitations](#limitations)

Status: [~]

## Meaning and forms

Noun + に関しての modifies a following nominal expression: “concerning;
regarding”. The stable card is 〜に関しての.

## Implementation and verification

`src/grammar_rules/rules/nikanshiteno/` matches adjacent noun, pronoun or
nominal suffix + に + 関し + て + の with particle and verb checks.
Only に関しての is reserved; attached vocabulary remains available.
Analyzer-backed tests cover 契約に関しての説明, それに関しての説明 and
二件に関しての報告, related constructions, missing attachment, alignment
gaps, card identity, frequency, example surfaces and reservation.
The broader 〜に関して / 〜に関する card continues to match independently.

## Limitations

Polite に関しましての, kana-only spellings and other tokenizations remain
outside verified coverage. The matcher does not validate the following nominal
expression. No corpus precision claim is made; this point remains partial.
