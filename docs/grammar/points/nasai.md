# 〜なさい

Status: [~]

## Meaning and tracked scope

An instruction or command formed with the verb's continuative stem. The rule
matches a source-adjacent continuative verb and the separate imperative
`なさい` token analyzed as `なさる`. Its example span is `なさい`.

## Implementation and evidence

- Rule: [nasai/index.py](../../../src/grammar_rules/rules/nasai/index.py)
- Tests: [nasai/test_index.py](../../../src/grammar_rules/rules/nasai/test_index.py)
- Registry identity: `nasai`; stable note identity: `〜なさい`.
- Sudachi full-dictionary tests cover 食べなさい, 読みなさい, しなさい,
  来なさい and 行きなさい, as well as lexical and honorific controls.
  `食べなさいます` has the same surface `なさい` but a continuative
  analysis, and is excluded. Standalone `話をなさい` is also excluded.
- The split greeting `お帰りなさい` matches structurally despite its greeting
  meaning. Single-token `おやすみなさい` and `ごめんなさい` do not match.
  Merged or differently analyzed commands and other spellings remain to be
  investigated; no corpus precision or recall evaluation has been performed.
  These limitations keep the point partial.

See [stage and precedence](../stage-and-precedence.md) and
[identity and review](../identity-and-review.md) for shared behavior.
