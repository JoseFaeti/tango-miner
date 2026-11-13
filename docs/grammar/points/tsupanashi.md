# 〜っぱなし

Status: [~]

## Meaning

An action continues, or its resulting state is left unchanged, often with a
negative implication.

## Implementation

- Rule: [tsupanashi](../../../src/grammar_rules/rules/tsupanashi/index.py)
- Tests: [tsupanashi tests](../../../src/grammar_rules/rules/tsupanashi/test_index.py)
- Registry identity: `tsupanashi`; stable expression: `〜っぱなし`.

Matches a separate `っぱなし` adjectival suffix after an adjacent continuative
verb, as in 開けっぱなし and 立ちっぱなし. The span covers the suffix alone,
leaving the verb and following copula or particle available. Tests cover Sudachi
full dictionary forms, lexical controls, card frequency and reservation.

Other tokenizations, including a merged verb and suffix, are unsupported.
Morphology cannot determine whether every occurrence implies neglect; corpus
precision and recall have not been measured.
