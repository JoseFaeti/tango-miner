# 〜ことはない

Status: [~]

## Meaning

No need to do something, or an event never happens. The rule matches a
directly adjacent non-past verb and split ことはない, including なかった,
ありません and ありませんでした. It reserves the construction while
retaining the verb. Both readings share one card; context determines the sense.

Nominal readings such as やることはない (there is nothing to do) are
indistinguishable from some grammar uses and can be matched. Past experience
行ったことはない and double-negative 行かないことはない are not matched;
other inflections and tokenizations remain uncovered. Independent precision
and recall evaluation is outstanding.

## Implementation

- Rule: [koto_wa_nai/index.py](../../../src/grammar_rules/rules/koto_wa_nai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/koto_wa_nai/test_index.py)
- Registry identity: `koto_wa_nai`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).
