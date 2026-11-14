# 〜つつ

Status: [~]

## Meaning

While or although.

## Implemented coverage

Matches adjacent continuative verbs and verbal auxiliaries followed by the
conjunctive particle つつ and a following clause.
The card reserves つつ while retaining the preceding verb as a lexical
candidate. The existing 〜つつある rule takes precedence for the progressive
forms it recognizes; 〜つつも takes precedence for its supported forms.
Sudachi tests cover 聴きつつ, 知りつつ, 知りつつも,
されつつ, progressive forms, 筒, spacing and incomplete clauses.

The matcher does not distinguish simultaneous from concessive meanings, and
accepts a following token as evidence of a clause without parsing its syntax.
Unrecognized つつある inflections and other tokenizations remain unverified.
