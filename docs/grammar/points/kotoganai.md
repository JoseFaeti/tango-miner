# 〜ことがない

Status: [~]

## Meaning

No experience or never happens. Past negative experience is covered by
[〜たことがある](ta-koto-ga-aru.md). The separate `koto_ga_nai` rule covers
non-past verbs followed by negative ことがない when preceded within 20 tokens
by 決して, けっして, 絶対に or ぜったいに. Sentence and quotation boundaries
end the cue window. It accepts merged ことがない and split negative present,
past and polite forms through ことがありませんでした, reserving only the
construction and retaining the verb.

The cue can modify another clause, and nominal readings remain ambiguous; it
does not prove the meaning. Uncued uses, other inflections and tokenizations
remain uncovered. Independent precision and recall evaluation is outstanding.

## Implementation

- Rule: [koto_ga_nai/index.py](../../../src/grammar_rules/rules/koto_ga_nai/index.py)
- Tests: [test_index.py](../../../src/grammar_rules/rules/koto_ga_nai/test_index.py)
- Registry identity: `koto_ga_nai`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).
