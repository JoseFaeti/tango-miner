# 〜につけては

- [Meaning](#meaning)
- [Supported behavior and limitations](#supported-behavior-and-limitations)
- [Implementation and evidence](#implementation-and-evidence)

Status: [~]

## Meaning

Concerning; with regard to.

## Supported behavior and limitations

Recognizes contiguous につけては after a noun, pronoun or verb. Supports
split に / つけ and combined につけ, followed by て / は or combined ては.
Only the marker is reserved; the preceding word remains available as vocabulary.
The shorter 〜につけ rule does not also match this construction.

Kanji 付ける and 着ける are excluded. Kana literal つける (attach, dip, etc.)
can have the same tokenization and remains ambiguous; no semantic filter is
applied. Verb attachment can also convey an occasion rather than the nominal
“concerning” reading. Corpus coverage and semantic disambiguation remain unevaluated.

## Implementation and evidence

- Rule: [nitsuketeha/index.py](../../../src/grammar_rules/rules/nitsuketeha/index.py), identity `nitsuketeha`.
- Tests: [test_index.py](../../../src/grammar_rules/rules/nitsuketeha/test_index.py).
- Sudachi full analyzes その件につけては後で話す。 as 件 / に / つけ / て / は
  around the marker. Analyzer regressions cover noun, pronoun and verb attachment,
  neighboring forms, kanji lexical controls and interrupted markers.
- Synthetic regressions cover combined-token forms and every internal gap;
  integration verifies reservation, example surface and registry/card identity.
