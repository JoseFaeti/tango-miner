# 〜反面 / 〜一方で

Status: [~]

## Meaning

Contrasting aspects or situations: “while”, “whereas”, “on the other hand”.

## Implementation

- Rule: [hanmen_itsupoude/index.py](../../../src/grammar_rules/rules/hanmen_itsupoude/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/hanmen_itsupoude/test_index.py)
- Registry identity: `hanmen_itsupoude`; expression: `〜反面 / 〜一方で`.

The rule matches 反面 or 一方で immediately after a verb, adjective, or
auxiliary in terminal or attributive form, when another clause begins before
sentence-ending punctuation. It covers forms such as 便利な反面 and
働く一方で. Sudachi's full dictionary analyzes 反面 and 一方 as nouns and
the で in 一方で as a case particle. Complete token boundaries protect
反面教師 and 一方通行. Matched tokens are reserved from lexical processing;
the preceding predicate remains available as lexical evidence.

This is partial coverage: sentence-initial その反面 and 一方で, noun + の,
and 一方 without で are not matched. A following token does not establish a
complete clause or semantic contrast, and 一方で can also describe simultaneous
events; these ambiguities need contextual review.

See [stage and precedence](../stage-and-precedence.md) and
[identity and review](../identity-and-review.md) for shared behavior.
