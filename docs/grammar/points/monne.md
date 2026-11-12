# 〜もんね

Status: [~]

## Meaning

Conversational explanation, often appealing to shared understanding or agreement.

## Tracked scope

Matches adjacent terminal or attributive verbs, adjectives, or auxiliaries
followed by nominal or sentence-final `もん` and sentence-final particle `ね`.
The span is `もんね`; this rule does not reserve component tokens, although
overlapping rules may reserve them. This includes noun
and na-adjective predicates with attributive `な`. A literal nominal reading,
such as a comment about something bought, can have the same form and remains a
review candidate. `ものね` and merged tokenizations are not covered. Corpus
precision and recall remain unverified.

## Implementation

- Rule and tests: [monne](../../../src/grammar_rules/rules/monne/)
- Registry identity: `monne`; stable identity: `〜もんね`.
