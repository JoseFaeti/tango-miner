# 〜ことは〜が

Status: [~]

## Meaning

Concession: acknowledges a proposition while qualifying it. The rule detects
an adjacent repeated verb, i-adjective or na-adjective around ことは followed
by が, けど, けれど or けれども and a following clause. It also handles a
single matching auxiliary, as in 行ったことは行ったが and 静かなことは静かだが.
It reserves ことは and retains both predicates as lexical evidence.

Matching is structural, not a semantic judgment; unusual repetitions may be
ambiguous. Longer predicate or auxiliary chains, intervening words, and other
connectors are not covered. Independent precision and recall evaluation remains.

## Implementation

- Rule: [koto_wa_ga/index.py](../../../src/grammar_rules/rules/koto_wa_ga/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/koto_wa_ga/test_index.py), including Sudachi forms, lexical controls, reservation and identity.
- Registry identity: `koto_wa_ga`; stable expression: `〜ことは〜が`.
