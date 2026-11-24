# 〜につれて

- [Meaning](#meaning)
- [Verified coverage](#verified-coverage)
- [Limitations](#limitations)

Status: [~]

## Meaning

As one change progresses, another change accompanies it.

## Verified coverage

The `nitsurete` rule matches adjacent nouns, pronouns and dictionary-form verbs
followed by につれて or に連れて under the canonical identity 〜につれて.
The preceding word remains lexical evidence. Sudachi-backed tests in
`src/grammar_rules/rules/nitsurete/test_index.py` verify 年齢に連れて変わる and
増えるに連れて変わる, alongside kana forms, boundaries, reservation and identity.
Kanji noun/pronoun attachments immediately followed by 行く, いく, 来る, くる,
帰る or 戻る are excluded: 学校に連れて行く／帰った／来る remains lexical.

## Limitations

This local destination guard does not resolve all literal accompaniment senses.
Kana literal uses such as 学校につれて行く still match. Kanji literal uses with
other predicates or intervening material can match; concurrent-change uses with
a noun and a guarded movement predicate can be missed. The shorter につれ／に連れ
and other tokenizations remain unsupported. The [kanji spelling](nitsurete-ee33e117.md)
shares this implementation and card identity.
