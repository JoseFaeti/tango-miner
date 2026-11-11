# 〜ていただけませんか

Status: [~]

## Meaning

Polite request: could you please do something?

## Implementation and limitations

The `te_itadakemasenka` rule recognizes an adjacent continuative verb or
auxiliary, connective て/で, potential いただける or 頂ける, negative
polite ません and question particle か. Analyzer tests cover kana and kanji,
て and で, a following ね, and incomplete or nonverbal controls. The request
span is reserved and the attached predicate remains eligible as vocabulary,
subject to shared dictionary expression precedence. Other overlapping grammar
rules can still produce cards for the same tokens.

Other request variants, merged tokenizations and corpus precision and recall
remain unevaluated. A grammatical match cannot establish whether the question
is intended as a request in context.

See [rule](../../../src/grammar_rules/rules/te_itadakemasenka/index.py),
[tests](../../../src/grammar_rules/rules/te_itadakemasenka/test_index.py) and
[stage and precedence](../stage-and-precedence.md).
