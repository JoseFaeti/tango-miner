# 〜んだ / 〜のだ

Status: [~]

## Meaning and tracked scope

explanatory construction.

  - Covered: supported ん/の + copula analyses and selected inflections.
  - Remaining: merged nominalizers, bare explanatory questions and other
    tokenizations; distinguish nominal and negative ambiguities.

## Implementation

- Rule: [src/grammar_rules/rules/nda.py](../../../src/grammar_rules/rules/nda.py)
- Tests: [tests/test_grammar_nda.py](../../../tests/test_grammar_nda.py)
- Registry identity: `explanatory_nda`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜んだ` card covers explanatory んだ, a common conversational
construction. Supported adjacent token analyses include ん/の with だ or です,
past, conjectural and common negative forms, and のである. The written の
form requires a preceding predicate to limit possessive matches. The rule also
recognizes single-token のだ and のです after a predicate, and a split の +
だった analysis. Highlights include the recognized ending. This is a
morphological cue, not a guarantee of the intended nuance: nominalized things
and some negative constructions can still be ambiguous. Other tokenizations,
including some where the nominalizer is merged with the preceding word, bare
explanatory questions, and unaligned streams can be missed. As with 〜ている,
a valid source example is required; other occurrences remain available as lexical candidates.
Existing notes with this exact identity are protected from automatic updates
and pruning, so their examples do not refresh automatically. Corpus diagnostics
recorded 10,894 split んだ analyses, but they were not independently evaluated
as grammar matches and do not measure this expanded rule's precision or recall.


## Dictionary precedence evidence

For explanatory `んだ`, a dictionary merge beginning at copular `な`
does not override the supported `な` + `ん` + copula analysis. This avoids
interpreting `だめなんだって` as interrogative `なんだって`. Longer
merged expressions such as `そうなんですか` may still take precedence,
and other ambiguous tokenizations remain unresolved by this rule. An ordered
20,000-sentence archive replay found 218 copular matches; the former precedence
rule preempted 141, including longer expressions, so that number is not a
measurement of the narrowed rule or a representative accuracy estimate.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
