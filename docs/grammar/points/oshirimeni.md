# 〜を尻目に

- [Meaning and scope](#meaning-and-scope)
- [Implementation and verification](#implementation-and-verification)
- [Limits](#limits)

Status: [x]

## Meaning and scope

Disregarding or showing indifference to someone or something while proceeding
with an action. Matches adjacent noun or pronoun + を尻目に, including kana
をしりめに. The grammar span starts at を and ends at に; the preceding
word remains lexical evidence. A following して does not change this span.

## Implementation and verification

Rule identity: `oshirimeni`; canonical expression: `〜を尻目に`.
See the [matcher](../../../src/grammar_rules/rules/oshirimeni/index.py) and
[tests](../../../src/grammar_rules/rules/oshirimeni/test_index.py).

Sudachi full dictionary in split mode C yields を / 尻目 / に and
を / しりめ / に. Constructed analyzer regressions cover both spellings,
nouns, pronouns, names, following punctuation and して, missing objects,
spaces, punctuation inside the expression and unrelated lexical uses.
Integration checks reservation, example surface, canonical and legacy identities,
and preservation of the preceding noun.

## Limits

The matcher recognizes the construction without judging the speaker's attitude.
Standalone 尻目 and 尻目にかける without a preceding object are not this
point. Alternative tokenizations are not supported. Verification uses constructed
sentences, not corpus precision or recall measurements.
