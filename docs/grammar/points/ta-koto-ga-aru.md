# 〜たことがある

Status: [~]

## Meaning and tracked scope

past experience.

  - Covered: verb + past auxiliary followed by merged ことがある or split
    こと + が + positive and negative present, past and polite forms,
    including merged ことがない.
  - Remaining: other inflections and tokenizations, nominal ambiguity, and
    independent precision and recall evaluation. Non-past occasional occurrence
    is handled separately and can also describe an existing matter.

## Implementation

- Rule: [src/grammar_rules/rules/koto_ga_aru.py](../../../src/grammar_rules/rules/koto_ga_aru.py)
- Tests: [tests/test_grammar_koto_ga_aru.py](../../../tests/test_grammar_koto_ga_aru.py)
- Registry identity: `koto_ga_aru_experience`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜たことがある` card recognizes a continuative verb and past auxiliary
followed by merged `ことがある` or split positive present, past and polite
forms through `ことがありました`; negative experience includes merged
`ことがない` and split `ことがなかった`, `ことがありません` and
`ことがありませんでした`. Negative forms can still have nominal readings.
A targeted replay found an existing-matter
reading in `いなくなってわかったことがあった`; that observed context is
left lexical. Other similar ambiguities remain unresolved.
It describes past experience and reserves the `ことがある` portion while
retaining the verb and past auxiliary. Unsupported inflections remain outside
this rule; ordinary nominal `こと`
and existence uses remain eligible for lexical processing. Precision and recall
have not been independently evaluated. The observed `困ったことがあるなら`
problem-expression context and observed `言い忘れたことがあるわ` are left
to lexical processing; other ambiguous
readings, including experience uses of `困る`, remain possible.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
