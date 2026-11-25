# 〜なしで済む

- [Meaning and supported form](#meaning-and-supported-form)
- [Verification](#verification)
- [Limitations](#limitations)

Status: [x]

## Meaning and supported form

An explicit noun or pronoun + なしで済む means “manage without X” or “not need X.”
The matcher accepts both なし and 無し and requires the head, なし, で, and a
form of 済む to be adjacent. Its span starts at なし, leaving the head available
as a separate vocabulary candidate. It shares the finite-form handling used by
〜て済む, including 済む / すむ, past, polite, and conditional forms.

This nominal construction is distinct from the verbal 〜ないで済む, 〜ずに済む,
and 〜なくて済む forms, which describe avoiding an action.

## Verification

Sudachi full dictionary tests cover noun and pronoun heads, なし / 無し, present,
past, polite and conditional forms. Controls cover a missing head, the lexical
noun 梨, other following verbs, a gap before 済む, なしでは, plain negative 済まない,
and the separate verbal 〜なくて済む form. A pipeline test checks reservation,
the grammar card identity, and retention of the head as vocabulary evidence.

## Limitations

The rule requires an overt, adjacent noun or pronoun head; it does not infer an
omitted referent. It identifies the local construction without interpreting
whether the sentence actually means that X was unnecessary. Colloquial なしじゃ
is outside its scope.
