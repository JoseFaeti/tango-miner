# Grammar implementation tracker

Ordered by estimated usefulness and frequency, not measured corpus counts.
Status: `[ ]` not implemented as a grammar card, `[~]` partially implemented,
`[x]` implemented to the intended scope and validated. A lexical candidate or
shadow proposal alone does not count as an implemented grammar card. Full
coverage does not require every conceivable tokenization; define the intended
scope and validate it before marking a point complete. Include common inflections and contractions where supported
by source evidence, and preserve ambiguous and useful lexical readings.

## Implemented with partial coverage

- [~] 〜ている — ongoing action or resulting state.
  - Covered: common plain, past, negative, polite and contracted forms.
  - Remaining: unsupported tokenizations and contractions; distinguish aspectual
    uses from lexical uses and independently evaluate precision and recall.
- [~] 〜んだ / 〜のだ — explanatory construction.
  - Covered: supported ん/の + copula analyses and selected inflections.
  - Remaining: merged nominalizers, bare explanatory questions and other
    tokenizations; distinguish nominal and negative ambiguities.
- [~] 〜たい — desire to perform an action.
  - Covered: supported split auxiliary forms, including common inflections.
  - Remaining: verb-plus-auxiliary merged tokenizations and other valid forms;
    evaluate ambiguity. 〜たがる is tracked separately.
- [~] 〜てしまう — completion or unintended result.
  - Covered: supported split forms and ちゃう/じゃう contractions, including
    common inflections.
  - Remaining: other tokenizations, inflections and contextual nuances.
- [~] 〜てくる — movement toward, return or emerging change.
  - Covered: supported split forms including past, negative, polite, て-form
    and volitional forms.
  - Remaining: merged expressions and ambiguous 〜てく; distinguish meanings
    and preserve lexical expressions.
- [~] 〜ものの — concession.
  - Covered: a single conjunction token after a supported predicate.
  - Remaining: other valid tokenizations and contexts; distinguish nominal
    もの + の and evaluate whether the surrounding context is concessive.

- [~] 〜ない — verbal negation.
  - Covered: supported split negative auxiliaries after irrealis verbs and
    selected passive, potential and causative auxiliaries.
  - Remaining: adjective negation, polite and other forms, merged analyses
    and independent precision and recall evaluation.

- [~] 〜たら — conditional or subsequent event.
  - Covered: split hypothetical たら / だら following supported continuative
    predicates and a following clause.
  - Remaining: sentence-final and other tokenizations, semantic distinctions,
    and independent precision and recall evaluation.

- [~] 〜ていく — movement away or change continuing onward.
  - Covered: supported split forms including past, negative, polite, て-form
    and volitional forms.
  - Remaining: merged expressions and ambiguous 〜てく; evaluate precision and recall.

- [~] 〜たことがある — past experience.
  - Covered: verb + past auxiliary followed by merged ことがある or split
    こと + が + positive and negative present, past and polite forms,
    including merged ことがない.
  - Remaining: other inflections and tokenizations, nominal ambiguity, and
    independent precision and recall evaluation. Non-past occasional occurrence
    is handled separately and can also describe an existing matter.

- [~] 〜ことがある — occasional occurrence.
  - Covered: non-past verbs with a nearby frequency cue and supported
    positive present, past and polite forms of ことがある.
  - Remaining: uncued occurrences, negative forms, other tokenizations, and
    distinguishing existing-matter readings; the cue does not prove its scope.

- [~] 〜ように — predicate-attached purpose, manner, resemblance or instruction.
  - Covered: selected merged and split tokenizations after a finite predicate.
  - Remaining: other forms, contextual disambiguation and independent evaluation.

All twelve rules require eligible source examples, cover only supported analyses,
and need independent precision and recall evaluation. See the
[grammar card details](grammar-shadow-prototype.md#grammar-cards).

## Shared inflection improvements

Prioritize these cross-rule improvements using observed source tokenizations.
Shared matching should let each construction choose applicable forms and meanings;
a successful morphological match does not by itself establish the intended
interpretation.

1. [~] Recognize complete conditional and colloquial
   negative sequences.
   Evaluate complete forms including `〜なきゃ` and `〜なかったら` for
   `〜ている`, `〜ていく`, `〜てくる`, `〜たい` and `〜んだ`, preserving
   independently useful lexical expressions and construction-specific boundaries.
   The shared matcher now consumes observed `なくっ` + `て` and split
   `なけれ` + `ば` completely; incomplete stems are rejected. Existing
   `なきゃ` and `なかったら` coverage is tested across selected rules.
   Sudachi’s `なくっ` + `た` + `ってな` analysis remains unsupported, and
   A same-input run found `〜ていく` down two and lexical `いく` and
   `ない` each up two; the Anki preview had no deletions. Other effects on
   expression precedence and recall still need investigation.
2. [~] Extend positive verb inflection handling for observed stem and
   tokenization patterns used by `〜ていく`, `〜てくる` and `〜てしまう`.
   These rules already cover many common past and polite forms; moving existing
   logic alone is a refactor, not evidence of improved coverage.
   `〜てしまって`, `〜ちゃって` and `〜じゃって` now include an
   adjacent separate connective `て`. A raw replay of one shadow archive
   found 1,049 such matches, up from zero. A same-input run increased the
   grammar frequency by 1,049, with corresponding decreases in `しまう`,
   `ちゃう` and `じゃう`. `ても`, `ては` and `てもいい` also fell;
   overlapping expression reservation needs investigation. The Anki preview
   reported no deletions.
   Fused `てる` tokenizations and other positive forms remain future work.
3. [~] Consolidate and extend polite-suffix handling for supported `ます`,
   `ました`, `ません` and `ませんでした` tokenizations. A shared matcher
   handles selected split and whole-suffix analyses, with callers choosing
   positive, negative, connective or volitional forms. It rejects incomplete stems and
   incomplete `ませんでし`; tentative shadow proposals retain their previous
   incomplete-span behavior. In the first 20,000 archived sentences, raw rule
   replay includes `ていきました`, `てきます`, `てません` and `てました`.
   A same-input comparison of the initial implementation found 25 frequency
   changes, including decreases of 74 for `〜ていく`, 35 for `〜てしまう`,
   14 for `〜てくる` and 5 for `〜ている`, with `ます` up 127. The
   preview had no deletions. This baseline also predates the separate
   `〜てしまって` change, so these differences cannot all be attributed to
   polite handling. An archived `買ってきましょか` example exposed loss of
   volitional `ましょ` and `ましょう`; these are now preserved by applicable
   rules. A same-input rerun after the correction restored `〜ていく` by 74,
   `〜てしまう` by 34, `〜てくる` by 14 and `〜ている` by 4; `ます` fell
   by 126. The `買ってきましょか` example returned. No candidates were added,
   removed or changed status, and the Anki preview again had no deletions.
   Compared with the earlier baseline, `〜ている` and `〜てしまう` each
   remain down one, while `ちゃう`, `てる`, `ます` and `まへん` are each up
   one; their source occurrences and expression precedence remain unexamined.
   Whole-suffix analyses need further source evidence; precision and recall
   remain unmeasured.
4. [~] Add reusable adjective-like inflection handling for forms such as
   `〜たかった`, `〜たくない` and `〜たければ`, supporting `〜たい`
   and future applicable constructions without conflating their meanings.
   The shared matcher accepts caller-selected forms and surfaces; `〜たい` uses
   it for its existing forms. A replay of 20,000 archived sentences found 343
   matches and no changed spans compared with the previous rule. This is a
   behavior-preserving refactor, not evidence of improved coverage or precision.
   Other tokenizations and construction-specific meanings remain to be evaluated;
   expression precedence, candidate frequencies, examples and Anki plans were
   not compared in a full pipeline run.

For each change, inspect archived source examples, test positive and negative
forms and useful lexical controls, and compare old and new matches and reserved
spans. Assess expression precedence, candidate frequencies, examples and Anki
plans; a larger count alone is not a recall or precision evaluation. Run the
pipeline test suite and record remaining ambiguity and partial coverage.

## Next candidates

- [~] 〜と思う — predicate quotation and intention; reserves matched 思う while retaining independent lexical uses. Quoted questions, noun predicates and other inflections remain unsupported.
- [~] 〜なければならない / 〜なくてはいけない — obligation. Covered: selected complete present and past, plain and polite negative endings, including なきゃ, なくちゃ and ないと. Merged verb-plus-suffix analyses, conditional past endings and elliptical forms remain unsupported; independent precision and recall evaluation remains needed.
- [~] 〜てもいい — permission.
  - Covered: verb continuative + merged てもいい or split て/で + も + いい/よい/良い, including supported past forms. Other readings and inflections remain lexical.
- [~] 〜てはいけない — prohibition. Covered: verb continuative + ては / では / ちゃ / じゃ + selected complete plain and polite present and past forms of いけない. Other tokenizations, inflections and contextual ambiguity remain to be evaluated.
- [~] 〜ておく — preparation or leaving a state.
  - Covered: split て/で + おく and auxiliary とく/どく, including selected
    plain, past, negative, polite, て-form, conditional and volitional forms.
  - Remaining: ambiguous analyses such as 読んどいた, merged tokens, negative
    antecedents, 置く spelling and other inflections. Evaluate lexical expression precedence,
    contextual meanings and independent precision and recall. Longer JMdict
    expressions beginning at おく take precedence; effects on output frequencies
    need a same-input corpus comparison.
- [~] 〜てみる — try doing something.
  - Covered: adjacent continuative verb + て/で + non-independent みる,
    including selected past, negative and polite forms.
  - Remaining: other tokenizations and inflections; distinguish literal “see”
    readings and evaluate precision and recall independently.
- [ ] 〜ことになる — decided outcome or consequence.
- [ ] 〜ことにする — decision to do something.
- [ ] 〜ば — conditional; distinguish other uses.
- [ ] 〜なら — conditional or topic-based supposition.
- [ ] 〜ても — concession, “even if”.
- [ ] 〜そうだ — appearance and hearsay; treat as distinct constructions.
- [ ] 〜ようだ / 〜みたいだ — resemblance or inference; distinguish uses.
- [ ] 〜はずだ — expectation or inference.
- [ ] 〜わけだ / 〜わけではない — explanation and partial negation; distinguish uses.
- [ ] 〜たがる — apparent desire of another person.
- [ ] 〜ながら — simultaneous actions; distinguish concessive uses.
- [ ] 〜ために — purpose or cause; distinguish meanings.
- [ ] 〜のに — concession or purpose; distinguish meanings and nominal uses.
- [ ] 〜かもしれない — possibility.

## Further candidates

These bands are rough implementation priorities, not frequency rankings or
claims of exhaustive coverage. Each line is a proposed family; split distinct
meanings or identities during implementation. Basic particles and inflections
may be useful as grammar targets, but their sheer frequency does not by itself
make separate cards useful. Verify examples, variants and ambiguity first.

### Common foundations and conversation

See [common grammar candidates](grammar/common-candidates.md).

### Less common, formal and literary constructions

See [formal and literary grammar candidates](grammar/formal-candidates.md).

## Reference and scope

[OpenJLPT](https://github.com/evanclan/OpenJLPT) reports 526 grammar points
and supplies a useful cross-check; its levels are approximate. This checklist
is project-authored and is not an import of its descriptions or examples.
It includes ordinary grammar, expressions and colloquial forms beyond JLPT
coverage. Some entries overlap or need verification and consolidation; their
presence is a lead for investigation, not proof that a separate card is useful.
Prioritize real source occurrences and assess ambiguity and recall before
implementing each rule.

