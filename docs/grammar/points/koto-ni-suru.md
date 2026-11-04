# 〜ことにする

Status: [~]

## Meaning and tracked scope

decision to do something. Covered: finite verb and verbal negative + merged or split construction, with selected present, past and polite positive forms. Continuing and negative endings, other tokenizations and ambiguous readings remain to be evaluated.

## Implementation

- Rule: [src/grammar_rules/rules/koto_ni_suru.py](../../../src/grammar_rules/rules/koto_ni_suru.py)
- Tests: [tests/test_grammar_koto_ni_suru.py](../../../src/grammar_rules/rules/tests/test_grammar_koto_ni_suru.py)
- Registry identity: `koto_ni_suru`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

### 〜ことにする

The card identifies a decision to do or not do something after a finite verb
or supported verbal negative, followed by merged ことにする or split こと +
に + する. Selected present, past and polite positive endings are supported.
The predicate remains available as a lexical candidate; the matched suffix is
reserved, subject to dictionary expression precedence. Noun-based uses,
continuing ことにしている, negative endings and other analyses remain lexical.
A decision reading is context dependent; precision and recall are not measured.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
