# 〜を押して

[Meaning](#meaning) · [Verified coverage](#verified-coverage) · [Limitations](#limitations)

Status: [~]

## Meaning

Despite; in spite of; braving an obstacle.

## Verified coverage

Matches adjacent noun + を押して, including kana をおして, when the noun's
surface or lemma is 病, 病気, 風邪, 熱, 高熱, 怪我, けが, 反対, 抵抗, 無理,
悪天候 or 困難. Only を押して is reserved; the obstacle remains vocabulary.
The registered card is 〜を押して (をおして).

Sudachi full dictionary regressions in
`src/grammar_rules/rules/ooshite/test_index.py` verify illness, injury,
opposition, weather, difficulty and 無理 contexts, kana spelling, complete
boundaries, reservation, example surfaces and identity. Controls retain literal
button, door, back and stamp uses, 推して and the separate を押し切って form.

## Limitations

The obstacle set deliberately bounds this initial rule because を押して alone
also means literal pushing. Other nouns, inflections and alternative tokenizations
remain unsupported. Contexts such as 熱 can still be ambiguous; the rule does
not infer intent from the sentence. Tests use constructed examples, not a
corpus precision or recall measurement. Further source evidence is needed
before expanding coverage.
