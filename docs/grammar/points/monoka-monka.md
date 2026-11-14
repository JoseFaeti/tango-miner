# 〜ものか / 〜もんか

Status: [~]

## Meaning

Strong denial or rhetorical disbelief and perceived impossibility.

## Tracked scope

Matches adjacent terminal or attributive verb, adjective, or auxiliary followed
by nominal `もの` or colloquial `もん` and sentence-final `か`. The span covers
`ものか` or `もんか`; tokens are not reserved. Sudachi full-dictionary tests
cover both spellings, predicate forms, and lexical controls.

Literal questions such as `買ったものか` share this morphology and remain
review candidates. A question about an ongoing state introduced by `あるいは`
is excluded as a speculative alternative; this heuristic may also exclude a
genuine rhetorical denial in that context. Rhetorical questions about ability,
such as `守れるものか`, remain candidates. Indirect questions with non-final `か`
are excluded.
Merged tokenizations and corpus precision and recall have not been evaluated.

## Implementation

- Rule and tests: [monoka_monka](../../../src/grammar_rules/rules/monoka_monka/)
- Registry identity: `monoka_monka`; stable identity: `〜ものか / 〜もんか`.
