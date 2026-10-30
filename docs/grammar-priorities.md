# Priority grammar implementation tracker

This is the short list of common or otherwise useful points to implement first,
ordered by estimated usefulness and frequency, not measured corpus counts.
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
- [~] 〜ことになる — decided outcome or consequence. Covered: finite verb and verbal negative + merged or split construction, including selected past and polite forms. Other inflections and noun readings remain lexical; precision and recall need evaluation.
- [~] 〜ことにする — decision to do something. Covered: finite verb and verbal negative + merged or split construction, with selected present, past and polite positive forms. Continuing and negative endings, other tokenizations and ambiguous readings remain to be evaluated.
- [~] 〜ば — conditional. Covered: split ば after hypothetical verbs, adjectives and auxiliaries with a following clause. Complete supported obligation forms use their own card. Sentence-final, merged and other analyses remain unsupported; expression precedence and independent precision and recall need evaluation.
- [ ] 〜なら — conditional or topic-based supposition.
- [ ] 〜ても — concession, “even if”.
- [ ] 〜そうだ — appearance and hearsay; treat as distinct constructions.
- [~] 〜ようだ / 〜みたいだ — resemblance or inference. Covered: 〜ようだ with selected predicate and noun attachments and present or past plain/polite copulas. 〜みたいだ, negative, attributive and continuative forms and other analyses remain unsupported; evaluate contextual meanings and recall.
- [ ] 〜はずだ — expectation or inference.
- [ ] 〜わけだ / 〜わけではない — explanation and partial negation; distinguish uses.
- [ ] 〜たがる — apparent desire of another person.
- [ ] 〜ながら — simultaneous actions; distinguish concessive uses.
- [ ] 〜ために — purpose or cause; distinguish meanings.
- [ ] 〜のに — concession or purpose; distinguish meanings and nominal uses.
- [ ] 〜かもしれない — possibility.

## Related work

[Shared inflection improvements](grammar/inflection-improvements.md) tracks
cross-rule changes and their evidence.

## Remaining candidates

The [remaining grammar candidates](grammar/remaining-candidates.md) are a
broader, unordered backlog. Some may be common or become priorities after
source evidence and implementation tradeoffs are assessed. Overlap with this
tracker does not imply a separate card or an unimplemented form.

## Reference and scope

[OpenJLPT](https://github.com/evanclan/OpenJLPT) reports 526 grammar points
and supplies a useful cross-check; its levels are approximate. This checklist
is project-authored and is not an import of its descriptions or examples.
It includes ordinary grammar, expressions and colloquial forms beyond JLPT
coverage. Some entries overlap or need verification and consolidation; their
presence is a lead for investigation, not proof that a separate card is useful.
Prioritize real source occurrences and assess ambiguity and recall before
implementing each rule.

