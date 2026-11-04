# 〜ように

Status: [~]

## Meaning and tracked scope

predicate-attached purpose, manner, resemblance or instruction.

  - Covered: selected merged and split tokenizations after a finite predicate.
  - Remaining: other forms, contextual disambiguation and independent evaluation.

## Implementation

- Rule: [src/grammar_rules/rules/you_ni/index.py](../../../src/grammar_rules/rules/you_ni/index.py)
- Tests: [tests/test_grammar_you_ni.py](../../../src/grammar_rules/rules/you_ni/test_index.py)
- Registry identity: `you_ni`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜ように` card recognizes selected merged and split `ように` analyses
after an adjacent finite verb, adjective or auxiliary, with following text. It
can express purpose, manner, resemblance or instruction; morphology does not
distinguish these meanings. Noun comparisons and other tokenizations remain
lexical. Dictionary expression precedence applies, and matched construction
tokens are reserved while the preceding predicate remains eligible. Precision
and recall have not been independently evaluated.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
