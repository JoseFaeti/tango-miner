# Additional card rules

[Grammar card index](card-details.md)

The `〜ない` card covers split verbal negative auxiliaries after an irrealis
verb or selected passive, potential and causative auxiliaries. It includes
`ない`, `なかった`, `なく`, and split `なければ`; following `で` and
`て` remain outside the span. Independent adjectival `ない` and `無い`
remain lexical, and adjective negation, polite negatives and merged analyses
are not covered. Precision and recall have not been evaluated across a corpus.

The `〜ていく` card recognizes supported split verb + connective + いく /
行く / ゆく forms, including past, negative, polite, て-form and volitional
forms. It covers movement away and continuing change without distinguishing
those meanings. Merged expressions and ambiguous contracted `〜てく` remain
unrecognized; independent precision and recall evaluation remains needed.

The `〜ことがある` card recognizes a non-past verb with `時々`, `ときどき`,
`たまに`, `偶に`, `時には` or `ときには` within the preceding 20 tokens,
stopping at sentence or quotation boundaries. It accepts merged `ことがある`
and split positive present, past and polite forms through `ことがありました`; negative experience includes merged
`ことがない` and split `ことがなかった`, `ことがありません` and
`ことがありませんでした`. Negative forms can still have nominal readings.
A targeted replay found an existing-matter
reading in `いなくなってわかったことがあった`; that observed context is
left lexical. Other similar ambiguities remain unresolved.
It reserves only the construction, leaving the verb and cue eligible. A cue
can modify another clause or describe an existing matter, so it does not prove
the intended meaning. Negative, uncued and other forms remain lexical. A replay
of an older 521,821-sentence shadow archive found nine raw rule matches; this
does not apply expression precedence or example eligibility, and is not an
independent precision or recall evaluation. Dictionary expression precedence
applies during a normal run. The shared `positive_verb_end` helper recognizes
only explicitly supplied verb forms; each rule must choose its own supported
inflections and semantic constraints. The shared `negative_predicate_end`
helper supports selected plain and polite negative forms. The shared negative
suffix matcher is also used by verbal negation, desire, explanatory, aspect,
direction and completion rules. Rules explicitly choose supported forms and
continue to handle their own contractions and meanings. These rules retain
previously recognized conditional, colloquial and dialectal negative token
boundaries, including some partial spans; they do not claim full construction
coverage or resolve lexical expression ambiguity.

The `〜たことがある` card recognizes a continuative verb and past auxiliary
followed by merged `ことがある` or split positive present, past and polite
forms through `ことがありました`; negative experience includes merged
`ことがない` and split `ことがなかった`, `ことがありません` and
`ことがありませんでした`. Negative forms can still have nominal readings.
A targeted replay found an existing-matter
reading in `いなくなってわかったことがあった`; that observed context is
left lexical. Other similar ambiguities remain unresolved.
It describes past experience and reserves the `ことがある` portion while
retaining the verb and past auxiliary. Unsupported inflections remain outside
this rule; ordinary nominal `こと`
and existence uses remain eligible for lexical processing. Precision and recall
have not been independently evaluated. The observed `困ったことがあるなら`
problem-expression context and observed `言い忘れたことがあるわ` are left
to lexical processing; other ambiguous
readings, including experience uses of `困る`, remain possible.

The `〜たら` card recognizes the hypothetical auxiliary `たら` / `だら`
after an adjacent continuative verb, adjective, or auxiliary when another clause
begins afterward. It covers conditions and events followed by a later outcome;
it does not distinguish these meanings. Conjunction analyses, sentence-final
uses, and other tokenizations remain unrecognized. The rule has not undergone
independent precision or recall evaluation.

The `〜ようだ` card recognizes selected finite predicate or noun + の attachments followed by split よう + だ / です, including だった and でした. It covers inference and resemblance without distinguishing them. Negative, attributive and continuative forms, みたいだ, other tokenizations and demonstrative このようだ remain lexical; independent precision and recall evaluation remains needed.

The `〜ように` card recognizes selected merged and split `ように` analyses
after an adjacent finite verb, adjective or auxiliary, with following text. It
can express purpose, manner, resemblance or instruction; morphology does not
distinguish these meanings. Noun comparisons and other tokenizations remain
lexical. Dictionary expression precedence applies, and matched construction
tokens are reserved while the preceding predicate remains eligible. Precision
and recall have not been independently evaluated.

### 〜と思う

The `〜と思う` card matches a source-aligned finite or volitional verb,
adjective, or auxiliary followed by と and 思う, 思った, 思います,
思いました, 思わない, 思わなかった, or 思って. It describes thought or
judgment, with intention possible after volitional forms. Quoted questions,
bare noun predicates, other inflections, and spaced token boundaries are not
covered. The full matched span supplies the example and expression precedence,
and its tokens are reserved from lexical collection. The attached predicate
and independent uses of 思う remain eligible.
The meaning remains context-dependent; this is not a semantic classifier.

### 〜てもいい

The permission card recognizes an adjacent continuative verb followed by merged
`てもいい` or split `て/で` + `も` + `いい`, `よい` or `良い`, including
`よかった`/`良かった`. It reserves the construction while retaining the verb;
a complete dictionary expression can take precedence. Noun + `でもいい` and
negative or other inflections are left to lexical processing. Depending on context,
the construction can express acceptance or concession as well as permission.

### 〜てはいけない

A source-aligned verb continuative followed by ては / では or contracted
ちゃ / じゃ and いけない expresses prohibition. Selected complete plain
and polite present and past endings are supported; the verb remains available
for lexical processing. Other forms and tokenizations remain lexical, and
contextual ambiguity, expression precedence and corpus-level recall remain
unmeasured.
