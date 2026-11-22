# 〜にあたって / 〜に際して

[Meaning](#meaning) · [Verified coverage](#verified-coverage)

Status: [~]

## Meaning

On the occasion of.

## Verified coverage

Matches adjacent nouns, pronouns and dictionary-form verbs followed by
にあたって, に当たって or に際して. The preceding word remains lexical
evidence. Focused tests cover Sudachi tokenization, boundaries, reservation and
card identity, including に当たっての, にあたっての and に際しての; の is
outside this rule's span, but に際しての also has a separate card reserving it.
For 当たる, the rule excludes 人 and 壁
and adjacent auxiliaries such as いる, ください and もらう to avoid attested
lexical uses. Duty nouns remain eligible in occasion contexts. This heuristic
can miss occasion readings with excluded nouns or continuations, and other
lexical uses can still match.
に際して is unaffected. Other inflections and tokenizations are not supported.
