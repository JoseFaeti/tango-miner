# 〜ている

Status: [~]

## Meaning and tracked scope

ongoing action or resulting state.

  - Covered: common plain, past, negative, polite and contracted forms.
  - Remaining: unsupported tokenizations and contractions; distinguish aspectual
    uses from lexical uses and independently evaluate precision and recall.

## Implementation

- Rule: [src/grammar_rules/rules/te_iru/index.py](../../../src/grammar_rules/rules/te_iru/index.py)
- Tests: [tests/test_grammar_te_iru.py](../../../src/grammar_rules/rules/te_iru/test_index.py)
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

Volitional and imperative forms `ていよう` / `でいよう`, `ていろ` /
`でいろ`, their `居` spellings, and contracted `てよう` / `でよう` and
`てろ` / `でろ` are recognized with the same preceding verb and token
boundary requirements. Focused Sudachi tests include locative `いよう` /
`いろ` and lexical `出よう` controls; these do not measure corpus-wide
precision or recall. Source-aligned conditional `ていれば` / `でいれば`, `居` spellings, and
contracted `てれば` / `でれば` are recognized when followed by connective
`ば`. Focused tokenizer tests cover locative `いれば` and lexical `出れば`
controls; other conditional tokenizations and corpus-wide precision and recall
remain unevaluated.

Source-aligned continuative `ていて` / `でいて` and contracted `てて` /
`でて` are also recognized when Sudachi identifies the final `て` as a
connective particle. Focused tokenizer regressions cover both forms and a
standalone locative `いて` control; they do not establish corpus-wide
precision or recall.

Colloquial `てん` / `でん` is recognized when Sudachi splits the
contracted auxiliary and nominalizing `ん`, as in an archived
`思ってんじゃねぇ` example. Fused auxiliary `てん` / `でん` followed by
nominalizing or sentence-final `の` is also recognized. The split `ん` is
included in the aspect span and can simultaneously belong to `〜んだ`;
focused tests check that overlap. Expression precedence and corpus-wide
frequency and precision remain unevaluated.

Source-aligned listing forms `ていたり` / `でいたり`, `居` spelling, and
contracted `てたり` / `でたり` are recognized when Sudachi analyzes
`たり` as an adverbial particle. Focused tokenizer tests include locative
`いたり` and lexical `出たり` controls. The particle is included in the
matched and reserved span; other analyses and corpus-wide accuracy remain
unevaluated.

A replay of the first 15,000 sentences in a full-corpus diagnostic archive found 25
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
