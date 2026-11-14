# 〜つつも

Status: [~]

## Meaning

Although.

## Implemented coverage

Matches a continuative verb or verbal auxiliary followed by adjacent つつも
and a following clause. The card reserves つつも and retains the preceding verb
as vocabulary; it takes precedence over the broader 〜つつ card. Tests cover
Sudachi tokenization, spacing, incomplete clauses, lexical 筒 and 〜つつある,
as well as card identity and reservation.

The matcher uses the presence of a following token as a clause heuristic and
does not establish the intended meaning in context. Other tokenizations and
corpus precision and recall remain unverified.
