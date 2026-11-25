# 〜を振り切って

- [Meaning and scope](#meaning-and-scope)
- [Implementation and verification](#implementation-and-verification)
- [Limits](#limits)

Status: [x]

## Meaning and scope

Shaking off or breaking free of something: opposition, hesitation, a pursuer,
or a physical restraint. Matches an attached object followed by を振り切って,
including をふりきって and を振りきって. The preceding object stays lexical;
the grammar span starts at を and ends at て.

## Implementation and verification

Rule identity: `ofurikitsute`; canonical expression: `〜を振り切って`.
See the [matcher](../../../src/grammar_rules/rules/ofurikitsute/index.py) and
[tests](../../../src/grammar_rules/rules/ofurikitsute/test_index.py).

Sudachi full dictionary in split mode C yields を / 振り切っ / て and
を / ふりきっ / て. Mixed spelling 振りきって yields 振り / きっ / て;
both token layouts are supported with adjacency and part-of-speech checks.
Constructed analyzer regressions cover opposition, pursuers, hesitation, literal
arm movement, following も, punctuation and spaces, missing objects, unrelated
verbs, and non-te inflections. Integration verifies token reservation, example
surface, canonical and legacy identities, and preservation of the object noun.

## Limits

Literal swinging-through uses such as 腕を振り切って share the same form and
are recognized; context is needed to distinguish the sense. No noun whitelist
is imposed. Other inflections of 振り切る remain lexical and are outside this
te-form point. Verification uses constructed sentences, not corpus precision
or recall measurements.
