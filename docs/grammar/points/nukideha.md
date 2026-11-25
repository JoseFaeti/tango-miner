# 〜抜きでは

- [Meaning and coverage](#meaning-and-coverage)
- [Implementation and verification](#implementation-and-verification)
- [Limits](#limits)

Status: [x]

## Meaning and coverage

Without an essential item or condition; often expresses that something cannot
be achieved or established without it. The rule recognizes an adjacent noun or
pronoun + 抜き / ぬき + では. It emits only 抜きでは / ぬきでは, leaving the
antecedent as lexical evidence. The related Nを抜きにしては construction is
separate. A grammar reference describes this point as Noun + 抜きでは and as
marking an outcome that cannot be realized without the condition. See the
[grammar reference][nukideha-reference].

## Implementation and verification

Rule identity: `nukideha`; canonical expression: `〜抜きでは`.
Sudachi full dictionary tests cover kanji and kana forms, pronoun and common
noun antecedents, spacing, missing antecedents, neighboring constructions,
and lexical compounds. Grammar-stage tests verify overlap with the shorter
〜ぬきで card, reservation, retained antecedent, and canonical and legacy
identities.

## Limits

The matcher requires a separate 抜き / ぬき noun token, so dictionary-merged
compounds such as 塩抜き may be missed. It matches token structure without
interpreting the following clause; some literal or lexical readings remain
ambiguous. Related 〜抜きには and 〜を抜きにしては forms are outside this rule.

[nukideha-reference]: https://aiueo.cc/pages_v2/ja/grammar/web/454.php
