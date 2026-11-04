# 〜ことがある

Status: [~]

## Meaning and tracked scope

occasional occurrence.

  - Covered: non-past verbs with a nearby frequency cue and supported
    positive present, past and polite forms of ことがある.
  - Remaining: uncued occurrences, negative forms, other tokenizations, and
    distinguishing existing-matter readings; the cue does not prove its scope.

## Implementation

- Rule: [src/grammar_rules/rules/koto_ga_aru_occasional.py](../../../src/grammar_rules/rules/koto_ga_aru_occasional.py)
- Tests: [tests/test_grammar_koto_ga_aru_occasional.py](../../../src/grammar_rules/rules/tests/test_grammar_koto_ga_aru_occasional.py)
- Registry identity: `koto_ga_aru_occasional`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜ことがある` card recognizes a non-past verb with `時々`, `ときどき`,
`たまに`, `偶に`, `時には` or `ときには` within the preceding 20 tokens,
stopping at sentence or quotation boundaries. It accepts merged `ことがある`
and split positive present, past and polite forms through `ことがありました`. Negative experience forms are handled by [〜たことがある](ta-koto-ga-aru.md), not this occasional-occurrence rule.

It reserves only the construction, leaving the verb and cue eligible. A cue
can modify another clause or describe an existing matter, so it does not prove
the intended meaning. Negative, uncued and other forms remain lexical. A replay
of an older 521,821-sentence diagnostic archive found nine raw rule matches; this
does not apply expression precedence or example eligibility, and is not an
independent precision or recall evaluation. Dictionary expression precedence
applies during a normal run. See [shared inflection improvements](../inflection-improvements.md) for matcher design and remaining cross-rule work.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
