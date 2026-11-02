# 〜ている

Status: [~]

## Meaning and tracked scope

ongoing action or resulting state.

  - Covered: common plain, past, negative, polite and contracted forms.
  - Remaining: unsupported tokenizations and contractions; distinguish aspectual
    uses from lexical uses and independently evaluate precision and recall.

## Implementation

- Rule: [src/grammar_rules/rules/te_iru.py](../../../src/grammar_rules/rules/te_iru.py)
- Tests: [tests/test_grammar_te_iru.py](../../../tests/test_grammar_te_iru.py)
- Registry identity: `te_iru`
- Registry and stable identities: [registry.py](../../../src/grammar_rules/registry.py), [GrammarConstants.py](../../../src/GrammarConstants.py).

## Existing implementation and evidence

Source occurrences with adjacent Sudachi continuative verb/auxiliary + connective て/で +
non-independent `いる`, or a contracted auxiliary `てる`/`でる`, are counted.
Supported forms include plain, past, negative, polite and some contracted
inflections, such as `でいた`, `ていなかった`, `ていませんでした`,
`てる` and `でた`. Contracted negatives include analyses such as `でない`,
`でなかった`, `でなく` and `でませんでした`, even when Sudachi merges
or mislabels their components. These surface and context cues can still be
ambiguous; other tokenizations remain unsupported. See [stage and precedence](../stage-and-precedence.md) for shared example eligibility, reservation and scoring.

Source-aligned continuative `ていて` / `でいて` and contracted `てて` /
`でて` are also recognized when Sudachi identifies the final `て` as a
connective particle. Focused tokenizer regressions cover both forms and a
standalone locative `いて` control; they do not establish corpus-wide
precision or recall.

A replay of the first 15,000 sentences in a full-corpus shadow archive found 25
matching occurrences after the continuative-form restriction. A prior version
also matched a name fragment that Sudachi analyzed as a non-continuative verb;
the restriction excludes that observed case. This bounded, ordered probe and
the three inspected examples are not a representative evaluation and cannot
establish precision or recall. An Anki-disabled small pipeline run produced the
new card and a source-linked, highlighted example.

After expanding the rule, a replay of the first 20,000 sentences in that
archive found 1,621 supported occurrences, including 755 `てる`, 403 `てた`,
43 `ていた`, and 42 `ている`. This ordered sample is biased toward the
first sources and is not a precision or recall evaluation. The latest completed
full run before this expansion counted 15,372 occurrences under the old rule;
it does not measure the expanded rule. A future run will produce new counts,
and existing Anki grammar notes retained their prior examples and frequency
under the policy in effect at that time.
The later contracted-negative extension found six `でない` and one
`でなかった` in the same 20,000-sentence replay. The observed examples
support those matches, but this small, ordered check does not establish
general precision or recall.

See [stage and precedence](../stage-and-precedence.md) and [identity and review](../identity-and-review.md) for shared behavior.

Related backlog family: [Contracted aspect 〜てる / 〜とる](teru-toru.md).
