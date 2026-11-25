# 〜を抜きにしては

Status: [x]

## Meaning and scope

Without something; unable to do something without it. Matches an adjacent noun
or pronoun followed by を抜きにしては, including ぬき. The span begins at を
and ends at は; the preceding item remains lexical evidence.

## Implementation and verification

Rule identity: `onukinishiteha`; canonical expression: `〜を抜きにしては`.
See the [matcher](../../../src/grammar_rules/rules/onukinishiteha/index.py)
and [tests](../../../src/grammar_rules/rules/onukinishiteha/test_index.py).

Sudachi full dictionary tests cover kanji and kana spellings, missing or spaced
は, spacing inside the construction, and an unrelated なくしては form. Grammar
stage tests verify the longer card alongside the existing 〜を抜きにして
prefix card, reserve the construction, preserve the antecedent, and retain
canonical and legacy identities.

## Limits

The matcher recognizes token structure without deciding whether the following
clause means something cannot happen without the antecedent; literal-purpose
readings can also match. Alternative tokenizations remain unsupported.
Verification uses constructed sentences, not corpus precision or recall
measurements.
