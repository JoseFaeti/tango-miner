# 〜間 / 〜間に

Status: [~]

## Meaning

Duration and occurrence within it.

## Verified behavior

- Adjacent attributive verbs, adjectives, auxiliaries, or nominal の before
  間 / あいだ produce one card for the duration and occurrence forms. The
  matched span includes に when present and retains the attached predicate or
  noun as lexical evidence. The tests use Sudachi split-mode C and verify card
  identity, reservation, examples, and attached-word retention.
- Nominal の間 also describes space or relationships (二人の間, 子供の間);
  these are indistinguishable by the token pattern and can produce false
  positives. The rule does not infer duration from the following predicate.
- 〜ている間に can also produce the existing 〜ている間に card; grammar
  matches overlap under the current pipeline. Bare and demonstrative uses such
  as その間に, other tokenizations, and further temporal variants remain
  unverified or outside this attached scope.
