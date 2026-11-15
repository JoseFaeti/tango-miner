# 〜というものだ

Status: [~]

## Meaning

That is what one calls; a judgment characterizing a situation.

## Tracked scope

Matches adjacent `という` or `っていう` (also `と言う`), nominal `もの`,
and `だ`, `です`, `だった`, or `でした`. The span starts at the quotation
particle. Full-dictionary tests cover these forms, competing rules and lexical
controls. Literal naming, such as `猫というものだ`, is indistinguishable
from a judgment and can also match. Active matches reserve their complete
tokens; longer matches suppress the overlapping `という` and `ものだ` rules. Dictionary
expression precedence can suppress grammar occurrences. Negative copulas,
other inflections and merged analyses remain unsupported; corpus precision
and recall have not been evaluated.

## Implementation

- Rule and tests: [toiumonoda](../../../src/grammar_rules/rules/toiumonoda/)
- Registry identity: `toiumonoda`; stable identity: `〜というものだ`.
