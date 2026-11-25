# 〜を抜きにしても

## Contents

- [Meaning and forms](#meaning-and-forms)
- [Implementation and verification](#implementation-and-verification)
- [Limits](#limits)

Status: [x]

## Meaning and forms

Even without or even setting aside the adjacent noun or pronoun. The matched
construction is を抜きにしても, also written をぬきにしても; the grammar span
starts at を and includes も, leaving the antecedent available as lexical
evidence.

## Implementation and verification

Rule identity: `onukinishitemo`; canonical expression: `〜を抜きにしても`.
See the [matcher](../../../src/grammar_rules/rules/onukinishitemo/index.py)
and [tests](../../../src/grammar_rules/rules/onukinishitemo/test_index.py).

Sudachi full dictionary tests cover kanji and kana spellings, positive and
negative clauses, the adjacent も ending, and controls for a missing or
different ending, spacing, and the related 〜を抜いても form. Stage tests verify
the full reserved span alongside the existing 〜を抜きにして card, preserve the
antecedent, and retain canonical and legacy identities.

## Limits

The matcher recognizes token structure; it cannot distinguish “setting aside”
from literal readings such as physically removing a tooth. Alternative
tokenizations are unsupported. Verification uses constructed examples and does
not measure corpus precision or recall.
