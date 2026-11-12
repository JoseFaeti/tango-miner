# 〜わけ / 〜ってわけ

Status: [~]

## Meaning

Explanatory ending.

## Verified coverage

Bare predicate-attached わけ or 訳 at the end of a sentence or before
punctuation is recognized after an adjacent finite verb, adjective, or
auxiliary. The predicate remains lexical evidence. The related
[〜ってこと / 〜ってわけ](tsutekoto-tsutewake.md) rule handles contracted
ってわけ, and [〜わけだ](wake-da.md) handles explicit copulas under their
existing card identities. Focused analyzer, lexical control, reservation and
identity tests are in `wake_tsutewake/test_index.py`.

## Remaining limitations

Bare わけ can also mean a reason; context alone may not distinguish that
nominal reading. Noun predicates and endings with sentence-final particles
are not covered by this rule. Contracted forms retain the related card identity.
