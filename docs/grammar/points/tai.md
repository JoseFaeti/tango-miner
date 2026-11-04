# 〜たい

Status: [~]

## Meaning and tracked scope

desire to perform an action.

  - Covered: supported split auxiliary forms, including common inflections.
  - Remaining: verb-plus-auxiliary merged tokenizations and other valid forms;
    evaluate ambiguity. 〜たがる is tracked separately.

## Implementation

- Rule: [src/grammar_rules/rules/tai.py](../../../src/grammar_rules/rules/tai.py)
- Tests: [tests/test_grammar_tai.py](../../../src/grammar_rules/rules/tests/test_grammar_tai.py)
- Registry identity: `tai`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation notes

The `〜たい` card expresses a desire to perform an action. It
requires a source-aligned continuative verb followed by Sudachi's `たい`
auxiliary. It recognizes plain, past, negative, conditional and selected polite
forms, such as `たい`, `たかった`, `たくない`, `たければ` and `たいです`.
It also recognizes continuative `たく`, for example in `たくなる`. The
distinct `たがる` construction is not included. Some valid forms are missed
when the analyzer merges the verb and auxiliary, and morphological evidence
does not guarantee the intended interpretation. Other occurrences remain
available as lexical candidates and existing notes with this exact identity are protected like the
other grammar notes. An ordered replay of the first 20,000 archived sentences
found 343 matches; this is not an independent precision or recall evaluation.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.
