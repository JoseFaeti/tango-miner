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

- [ ] 〜と思う — thought or intention; retain lexical 思う.
- [ ] 〜なければならない / 〜なくてはいけない — obligation; include common contractions.
- [ ] 〜てもいい — permission.
- [ ] 〜てはいけない — prohibition; include 〜ちゃいけない / 〜じゃいけない.
- [ ] 〜ておく — preparation or leaving a state; include 〜とく / 〜どく.
- [ ] 〜てみる — try doing something.
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

- [ ] は (topic and contrast)
- [ ] が (subject and focus)
- [ ] を (object and route)
- [ ] に (time, destination, location and recipient)
- [ ] で (place, means and cause)
- [ ] へ (direction)
- [ ] と (quotation, accompaniment and listing)
- [ ] の (possession and nominalization)
- [ ] も (inclusion)
- [ ] から / まで (starting and end points)
- [ ] より (comparison)
- [ ] や / とか (non-exhaustive listing)
- [ ] だけ / のみ (only)
- [ ] しか〜ない (only)
- [ ] さえ / すら (even)
- [ ] ばかり (only, excessive or recent)
- [ ] ほど / くらい / ぐらい (degree)
- [ ] など / なんか / なんて (examples or emphasis)
- [ ] ね / よ / よね (sentence-final particles)
- [ ] か (question or uncertainty)
- [ ] な (self-directed or prohibitive sentence ending)
- [ ] の / ん (explanatory question or ending)
- [ ] ぞ / ぜ / わ (sentence-final emphasis)
- [ ] だ / です (copula and politeness)
- [ ] じゃない / ではない (negative copula)
- [ ] だった / でした (past copula)
- [ ] だろう / でしょう (conjecture or confirmation)
- [ ] ある / いる (existence)
- [ ] 〜ません / 〜ました / 〜ませんでした (polite inflection)
- [ ] 〜た (past or completed event)
- [ ] 〜て (connection, sequence and request)
- [ ] 〜てください / 〜てくれ (requests)
- [ ] 〜ないでください / 〜ないで (negative request or without doing)
- [ ] 〜ましょう / 〜よう (volition and invitation)
- [ ] 〜ませんか / 〜ないか (invitation)
- [ ] 〜なさい (instruction)
- [ ] 〜ろ / 〜な (imperative and prohibition)
- [ ] 〜れる / 〜られる (potential, passive and honorific; separate analyses)
- [ ] 〜せる / 〜させる (causative)
- [ ] 〜させられる (causative-passive)
- [ ] 〜てある (resulting state)
- [ ] 〜てあげる / 〜てくれる / 〜てもらう (benefactive)
- [ ] 〜てほしい (want someone to do)
- [ ] 〜ほしい (desire for something)
- [ ] 〜ていただく / 〜てくださる / 〜て差し上げる (polite benefactive)
- [ ] 〜ていただけませんか (polite request)
- [ ] 〜てくれない / 〜てもらえない (request)
- [ ] 〜てごらん (try or instruction)
- [ ] 〜てちょうだい (request)
- [ ] 〜てやる (benefactive or determination)
- [ ] 〜なくてもいい (no obligation)
- [ ] 〜ないと / 〜なくちゃ / 〜なきゃ (obligation)
- [ ] 〜なくては / 〜なければ (obligation, elliptical)
- [ ] 〜ほうがいい (advice)
- [ ] 〜たほうがいい / 〜ないほうがいい (recommendation)
- [ ] 〜ことができる (ability)
- [ ] 〜られる / 〜れる (ability, including colloquial potential)
- [ ] 〜やすい / 〜にくい (ease or difficulty)
- [ ] 〜すぎる (excess)
- [ ] 〜始める / 〜出す (beginning)
- [ ] 〜続ける (continuation)
- [ ] 〜終わる / 〜終える (completion)
- [ ] 〜かける (incomplete or about to)
- [ ] 〜直す (doing again)
- [ ] 〜合う (mutual action)
- [ ] 〜方 (way of doing)
- [ ] 〜ながらも (concession)
- [ ] 〜たり〜たりする (representative actions)
- [ ] 〜たりする (example or possibility)
- [ ] 〜し〜し (multiple reasons or examples)
- [ ] 〜から / 〜ので (reason)
- [ ] 〜けど / 〜けれども (contrast or softening)
- [ ] 〜が (contrast or introduction)
- [ ] 〜けどね / 〜けどさ (conversational ending)
- [ ] 〜と (conditional or habitual consequence)
- [ ] 〜とき (when)
- [ ] 〜前に / 〜後で (before and after)
- [ ] 〜てから (after doing)
- [ ] 〜までに (deadline)
- [ ] 〜まで (until)
- [ ] 〜間 / 〜間に (duration and occurrence within it)
- [ ] 〜うちに (while or before change)
- [ ] 〜ところだ (about to, in progress or just finished)
- [ ] 〜たばかり (just did)
- [ ] 〜たところ (just did or result of trying)
- [ ] 〜たことがある (past experience)
- [ ] 〜ことがない (no experience or never happens)
- [ ] 〜ことはない (no need or never)
- [ ] 〜つもりだ (intention)
- [ ] 〜予定だ (plan)
- [ ] 〜ようと思う (intention)
- [ ] 〜ようとする (attempt or about to)
- [ ] 〜ようになる (change in ability or habit)
- [ ] 〜ようにする (make an effort or habit)
- [ ] 〜ように言う / 頼む (indirect instruction or request)
- [ ] 〜ように見える (appearance)
- [ ] 〜みたいに / 〜みたいな (resemblance)
- [ ] 〜らしい (hearsay or characteristic)
- [ ] 〜っぽい (resemblance or tendency)
- [ ] 〜そうにない (unlikely appearance)
- [ ] 〜そうもない (unlikely)
- [ ] 〜そうになる (nearly happens)
- [ ] 〜という / 〜っていう (quotation, name or explanation)
- [ ] 〜って (quotation, topic or hearsay)
- [ ] 〜と言う / 〜と聞く (quotation or reported information)
- [ ] 〜ということだ (meaning or hearsay)
- [ ] 〜ということは (implication)
- [ ] 〜というのは (topic or explanation)
- [ ] 〜ってこと / 〜ってわけ (colloquial explanation)
- [ ] 〜というより (rather than)
- [ ] 〜かどうか (whether)
- [ ] 〜かもしれん / 〜かも (colloquial possibility)
- [ ] 〜に違いない (certainty)
- [ ] 〜はずがない (cannot be expected)
- [ ] 〜わけがない (impossibility)
- [ ] 〜わけにはいかない (cannot reasonably do)
- [ ] 〜わけではない / 〜わけじゃない (partial negation)
- [ ] 〜ものだ (general truth, recollection or admonition)
- [ ] 〜ものか / 〜もんか (strong denial)
- [ ] 〜ものだから / 〜もんだから (reason)
- [ ] 〜もの / 〜もん (conversational reason)
- [ ] 〜もんね (explanation or shared understanding)
- [ ] 〜ことだ (advice or exclamation)
- [ ] 〜ことか (exclamation)
- [ ] 〜ことに (speaker's reaction)
- [ ] 〜ことは〜が (concession)
- [ ] 〜ということになる (result or implication)
- [ ] 〜ては / 〜ちゃ / 〜じゃ (conditional or repeated action)
- [ ] 〜たりなんかして (tentative suggestion)
- [ ] 〜じゃん / 〜じゃないか (recognition or appeal)
- [ ] 〜んじゃない / 〜のではない (supposition, prohibition or negation)
- [ ] 〜じゃないと / 〜でないと (otherwise)
- [ ] 〜んだけど / 〜んですが (softened introduction)
- [ ] 〜んだって / 〜だって (reported speech or emphasis)
- [ ] 〜わけ / 〜ってわけ (explanatory ending)
- [ ] 〜っけ (recalling or confirming)
- [ ] 〜かな / 〜かしら (wondering)
- [ ] 〜かい / 〜だい (informal question)
- [ ] 〜のか / 〜んか (question or realization)
- [ ] 〜のだろうか (wondering)
- [ ] 〜かいな / 〜やん / 〜やろ (dialectal forms; distinguish dialect)
- [~] 〜てる / 〜とる (contracted aspect; distinguish dialect and meaning)
  - Covered: supported 〜てる / 〜でる forms under 〜ている.
  - Remaining: other contractions and 〜とる; distinguish dialect and meaning.
- [~] 〜ちゃう / 〜じゃう (contracted completion)
  - Covered: supported auxiliary contractions under 〜てしまう.
  - Remaining: other tokenizations and inflections; distinguish lexical uses.
- [ ] 〜てく / 〜てきた (direction or change; distinguish existing 〜てくる)
- [ ] 〜とか (quotation or uncertain report)
- [ ] 〜なんて (surprise, example or disparagement)
- [ ] 〜なんか (example or disparagement)
- [ ] 〜ばいい / 〜たらいい (suggestion or wish)
- [ ] 〜ばよかった / 〜たらよかった (regret)
- [ ] 〜たらどう / 〜たらどうですか (suggestion)
- [ ] 〜といい / 〜といいな (hope)
- [ ] 〜ないかな (wish or request)
- [ ] 〜たって / 〜だって (even if)
- [ ] 〜てもらいたい (want someone to do)
- [ ] 〜ことにしている (habitual decision)
- [ ] 〜ことになっている (rule or arrangement)
- [ ] 〜ことになった (decision or outcome)
- [ ] 〜なくなる / 〜ようになる (change of state)
- [ ] 〜くなる / 〜になる (becoming)
- [ ] 〜くする / 〜にする (making or choosing)
- [ ] 〜にしては (considering)
- [ ] 〜にしても (even assuming or even if)
- [ ] 〜にしても〜にしても (whether or)
- [ ] 〜として (as or in the capacity of)
- [ ] 〜としても (even supposing)
- [ ] 〜としたら / 〜とすれば (if one supposes)
- [ ] 〜とすると (assuming or consequently)
- [ ] 〜にとって (from the perspective of)
- [ ] 〜について (about)
- [ ] 〜に対して (toward or in contrast to)
- [ ] 〜によって / 〜による (by, depending on or due to)
- [ ] 〜によると / 〜によれば (according to)
- [ ] 〜に関して / 〜に関する (concerning)
- [ ] 〜に比べて (compared with)
- [ ] 〜に向かって / 〜に向けて (toward or aimed at)
- [ ] 〜に向いている / 〜向き (suited to)
- [ ] 〜に限る (best or limited to)
- [ ] 〜に限って (especially or only)
- [ ] 〜に限らず (not limited to)
- [ ] 〜に代わって / 〜の代わりに (instead of)
- [ ] 〜に加えて (in addition)
- [ ] 〜に合わせて (in accordance with)
- [ ] 〜に応じて (depending on)
- [ ] 〜に沿って (along or in accordance with)
- [ ] 〜に基づいて (based on)
- [ ] 〜を通して / 〜を通じて (through or throughout)
- [ ] 〜をめぐって (concerning or around)
- [ ] 〜にわたって (over a range or period)
- [ ] 〜にかけて (through a period or area; other senses)
- [ ] 〜につれて (as changes occur together)
- [ ] 〜にしたがって (as or according to)
- [ ] 〜に伴って (accompanying change)
- [ ] 〜たびに (whenever)
- [ ] 〜次第 (as soon as or depending on)
- [ ] 〜最中に (in the middle of)
- [ ] 〜ている間に (while)
- [ ] 〜かけだ (unfinished or in progress)
- [ ] 〜まま (remaining in a state)
- [ ] 〜っぱなし (left in a state)
- [ ] 〜きり (only or since)
- [ ] 〜以来 (since)
- [ ] 〜て以来 (ever since)
- [ ] 〜あげく (after a long process, resulting in)
- [ ] 〜末に (after a process)
- [ ] 〜結果 (as a result of)
- [ ] 〜おかげで (thanks to)
- [ ] 〜せいで (because of, undesirable result)
- [ ] 〜ため (reason or purpose)
- [ ] 〜からこそ (precisely because)
- [ ] 〜からには (now that)
- [ ] 〜以上は (given that)
- [ ] 〜上で (after or in doing)
- [ ] 〜上に (in addition)
- [ ] 〜うえは (now that)
- [ ] 〜反面 / 〜一方で (on the other hand)
- [ ] 〜一方だ (continual change)
- [ ] 〜かわりに (instead or in exchange)
- [ ] 〜だけでなく (not only)
- [ ] 〜ばかりでなく (not only)
- [ ] 〜はもちろん (not to mention)
- [ ] 〜どころか (far from)
- [ ] 〜どころではない (not the time for)
- [ ] 〜にすぎない (merely)
- [ ] 〜にほかならない (nothing other than)
- [ ] 〜としては (as for or in one's capacity)
- [ ] 〜において / 〜における (in or at)
- [ ] 〜を中心に (centered on)
- [ ] 〜をはじめ (starting with)
- [ ] 〜をもとに (based on)
- [ ] 〜をきっかけに (triggered by)
- [ ] 〜を契機に (taking as an opportunity)
- [ ] 〜を目的に (for the purpose of)
- [ ] 〜向け / 〜向き (intended for or suited to)
- [ ] 〜だらけ (covered with or full of)
- [ ] 〜まみれ (covered with)
- [ ] 〜がち (tendency)
- [ ] 〜気味 (slight tendency)
- [ ] 〜げ (appearance)
- [ ] 〜がる (outward signs of feeling)
- [ ] 〜っこない (cannot possibly)
- [ ] 〜ものなら (if it were possible)
- [ ] 〜ようものなら (if such a thing happened)
- [ ] 〜とはいえ (although)
- [ ] 〜といっても (although one says)
- [ ] 〜にもかかわらず (despite)
- [ ] 〜くせに (despite, with criticism)
- [ ] 〜わりに (considering)
- [ ] 〜としても / 〜にしても (even if)
- [ ] 〜としても〜としても (whether or)
- [ ] 〜にかかわらず (regardless of)
- [ ] 〜を問わず (regardless of)
- [ ] 〜はともかく (setting aside)
- [ ] 〜は別として (apart from)
- [ ] 〜を除いて (except)
- [ ] 〜を抜きにして (without)
- [ ] 〜ほかない / 〜しかない (no choice)
- [ ] 〜ざるを得ない (cannot avoid doing)
- [ ] 〜ないわけにはいかない (cannot avoid doing)
- [ ] 〜ずにはいられない (cannot help doing)
- [ ] 〜てたまらない (unbearably)
- [ ] 〜てしかたがない / 〜てしょうがない (cannot help feeling)
- [ ] 〜てならない (strong feeling)
- [ ] 〜てすむ (suffices to)
- [ ] 〜ないですむ (avoid having to)
- [ ] 〜かねる (find it difficult to)
- [ ] 〜かねない (might, undesirable outcome)
- [ ] 〜得る / 〜得ない (possibility or impossibility)
- [ ] 〜ようがない (no way to)
- [ ] 〜にくい / 〜づらい / 〜がたい (difficulty; distinguish nuances)
- [ ] 〜きる / 〜きれない (complete or unable to complete)
- [ ] 〜抜く (do thoroughly)
- [ ] 〜通す (persist through)
- [ ] 〜込む (enter or do deeply)
- [ ] 〜かけ / 〜かける (half-done)
- [ ] 〜つつある (ongoing change)
- [ ] 〜つつ (while or although)
- [ ] 〜つつも (although)
- [ ] 〜ずに (without doing)
- [ ] 〜ぬ / 〜ん (negative forms)
- [ ] 〜ず (negative continuative)
- [ ] 〜ずにすむ (manage without)
- [ ] 〜ないまま (without changing state)
- [ ] 〜ぬまま (without doing)
- [ ] 〜ないことには (unless)
- [ ] 〜ないことはない (not impossible)
- [ ] 〜ないでもない (not entirely impossible)
- [ ] 〜ないではいられない (cannot help doing)
- [ ] 〜ないではおかない (inevitably causes)
- [ ] 〜に決まっている (certainly)
- [ ] 〜に相違ない (certainly)
- [ ] 〜に決まってる (colloquial certainty)
- [ ] 〜はずだった (contrary expectation)
- [ ] 〜べきだ / 〜べきではない (ought or ought not)
- [ ] 〜ものではない (should not)
- [ ] 〜ものがある (something noteworthy)
- [ ] 〜というものだ (that is what one calls)
- [ ] 〜ものなら / 〜ものならば (hypothetical)
- [ ] 〜ようでは (if the situation is such)
- [ ] 〜ようではないか (invitation)
- [ ] 〜ではないか (appeal or realization)
- [ ] 〜ではあるまいか (tentative supposition)
- [ ] 〜まい (negative volition or conjecture)
- [ ] 〜まいか (negative conjectural question)
- [ ] 〜だろうに (conjecture with regret or contrast)
- [ ] 〜だろうか (question or doubt)
- [ ] 〜でしょうか (polite question)
- [ ] 〜ではないだろうか (tentative suggestion)

### Less common, formal and literary constructions

- [ ] 〜にあたって / 〜に際して (on the occasion of)
- [ ] 〜に先立って (prior to)
- [ ] 〜を機に (on the occasion of)
- [ ] 〜を皮切りに (starting with)
- [ ] 〜を境に (from a turning point)
- [ ] 〜をもって (with or as of)
- [ ] 〜をもってしても (even with)
- [ ] 〜を踏まえて (taking into account)
- [ ] 〜を背景に (against the background of)
- [ ] 〜を前提に (on the assumption of)
- [ ] 〜を通じて (through or over a period)
- [ ] 〜に即して (in accordance with)
- [ ] 〜に照らして (in light of)
- [ ] 〜に則って (following a rule)
- [ ] 〜に沿う (follow or conform to)
- [ ] 〜にかけては (in the field of)
- [ ] 〜に至るまで (up to and including)
- [ ] 〜に至って (upon reaching or only when)
- [ ] 〜に至る (come to)
- [ ] 〜に至っては (as for, particularly)
- [ ] 〜にあって (in the situation of)
- [ ] 〜にあっては (in the case of)
- [ ] 〜にあっても (even in)
- [ ] 〜にあらず (is not; literary)
- [ ] 〜である (formal copula)
- [ ] 〜であろう (formal conjecture)
- [ ] 〜ではなく (rather than)
- [ ] 〜であって (being or and)
- [ ] 〜であっても (even if)
- [ ] 〜であれ / 〜であろうと (even if or whoever)
- [ ] 〜たる (in the capacity of; literary)
- [ ] 〜たるもの (one who is)
- [ ] 〜たるや (as for)
- [ ] 〜にたる / 〜に足る (worthy of)
- [ ] 〜に足りない (not worth)
- [ ] 〜に値する (worthy of)
- [ ] 〜に難くない (not difficult to imagine)
- [ ] 〜に堪えない (cannot bear or deeply feels)
- [ ] 〜に堪える (withstands or worthy of)
- [ ] 〜に忍びない (cannot bear to)
- [ ] 〜に越したことはない (nothing better than)
- [ ] 〜に及ばない (no need or does not reach)
- [ ] 〜には及ばない (no need)
- [ ] 〜には当たらない (does not warrant)
- [ ] 〜にあたらない (does not constitute)
- [ ] 〜を余儀なくされる (be forced to)
- [ ] 〜を余儀なくする (force someone to)
- [ ] 〜を禁じ得ない (cannot suppress a feeling)
- [ ] 〜を免れない (cannot escape)
- [ ] 〜をものともせず (undeterred by)
- [ ] 〜を顧みず (without regard for)
- [ ] 〜をよそに (regardless of)
- [ ] 〜を差し置いて (passing over)
- [ ] 〜を限りに (ending with)
- [ ] 〜を最後に (after which no more)
- [ ] 〜をおいて (apart from)
- [ ] 〜をさておき (setting aside)
- [ ] 〜をはじめとして (starting with)
- [ ] 〜を中心として (centered on)
- [ ] 〜を兼ねて (serving also as)
- [ ] 〜を経て (after passing through)
- [ ] 〜を受けて (in response to)
- [ ] 〜を通して (through)
- [ ] 〜にかこつけて (using as a pretext)
- [ ] 〜にかたくない (easy to imagine)
- [ ] 〜にかまけて (preoccupied with)
- [ ] 〜にひきかえ (in contrast)
- [ ] 〜にひきかえ / 〜に比べ (in comparison)
- [ ] 〜に則り (in accordance with)
- [ ] 〜に則した (in accordance with)
- [ ] 〜に即した (suited to)
- [ ] 〜に基づく (based on)
- [ ] 〜に関わる (relating to or affecting)
- [ ] 〜に関わらず (regardless of)
- [ ] 〜いかんで / 〜いかんによっては (depending on)
- [ ] 〜いかんにかかわらず (regardless of)
- [ ] 〜いかんを問わず (regardless of)
- [ ] 〜如何では (depending on)
- [ ] 〜次第で (depending on)
- [ ] 〜次第だ (depends on)
- [ ] 〜次第では (depending on circumstances)
- [ ] 〜が早いか (as soon as)
- [ ] 〜や否や (as soon as)
- [ ] 〜なり (as soon as)
- [ ] 〜そばから (repeatedly as soon as)
- [ ] 〜かと思うと / 〜かと思ったら (immediately after)
- [ ] 〜か〜ないかのうちに (barely had happened when)
- [ ] 〜かたがた (at the same time as)
- [ ] 〜がてら (while also doing)
- [ ] 〜ついでに (taking the opportunity)
- [ ] 〜かたわら (alongside)
- [ ] 〜と同時に (at the same time as)
- [ ] 〜とともに (together with or as)
- [ ] 〜と相まって (combined with)
- [ ] 〜に加え (in addition)
- [ ] 〜のみならず (not only)
- [ ] 〜ばかりか (not only)
- [ ] 〜はおろか (not to mention)
- [ ] 〜どころではなく (far beyond)
- [ ] 〜まして (still more)
- [ ] 〜にまして (even more than)
- [ ] 〜にもまして (more than ever)
- [ ] 〜にもほどがある (there is a limit)
- [ ] 〜極まりない / 〜極まる (extremely)
- [ ] 〜の極み (the height of)
- [ ] 〜の至り (the utmost)
- [ ] 〜限りだ (extremely)
- [ ] 〜てやまない (earnestly and continuously)
- [ ] 〜てはばからない (openly without hesitation)
- [ ] 〜ずにはおかない (inevitably causes)
- [ ] 〜てかなわない (cannot bear)
- [ ] 〜てしかるべきだ (naturally should)
- [ ] 〜べく (in order to)
- [ ] 〜べくして (as expected)
- [ ] 〜べからず (must not; literary)
- [ ] 〜べからざる (must not; attributive)
- [ ] 〜べきではない (ought not)
- [ ] 〜べきか (whether one should)
- [ ] 〜んばかりに (as if about to)
- [ ] 〜んがために (in order to; literary)
- [ ] 〜んとする (about to or intending to; literary)
- [ ] 〜んとばかりに (as though saying)
- [ ] 〜ずくめ (entirely)
- [ ] 〜ずじまい (ended without doing)
- [ ] 〜ずとも (even without)
- [ ] 〜ずにはすまない (cannot get away without)
- [ ] 〜ずして (without; formal)
- [ ] 〜ずともよい (need not; literary)
- [ ] 〜ずもがな (better left unsaid)
- [ ] 〜ないまでも (even if not)
- [ ] 〜ないものでもない (not impossible)
- [ ] 〜ないではすまない (cannot avoid)
- [ ] 〜ないに越したことはない (best not to)
- [ ] 〜ないこともない (possible, with reservation)
- [ ] 〜ないことには始まらない (nothing starts unless)
- [ ] 〜なくして (without)
- [ ] 〜なくしては (without, cannot)
- [ ] 〜なしに / 〜なしでは (without)
- [ ] 〜ぬきで / 〜ぬきにして (without)
- [ ] 〜といえども (even though)
- [ ] 〜といえど (even though; literary)
- [ ] 〜とはいっても (even so)
- [ ] 〜とはいうものの (although)
- [ ] 〜とはいえない (cannot say)
- [ ] 〜とは限らない (not necessarily)
- [ ] 〜ないとも限らない (might happen)
- [ ] 〜とも限らない (not necessarily)
- [ ] 〜ともなると (when it comes to)
- [ ] 〜ともあろう者が (someone of that standing)
- [ ] 〜ともあれ (be that as it may)
- [ ] 〜ともなく / 〜ともなしに (without intending to)
- [ ] 〜といわず〜といわず (both and everything)
- [ ] 〜といい〜といい (both examples)
- [ ] 〜だの〜だの (listing with attitude)
- [ ] 〜なり〜なり (alternative examples)
- [ ] 〜やら〜やら (uncertain or multiple items)
- [ ] 〜につけ〜につけ (whether or)
- [ ] 〜につけ (whenever or on the occasion of)
- [ ] 〜につけても (even in relation to)
- [ ] 〜にせよ / 〜にしろ (even if or whichever)
- [ ] 〜にせよ〜にせよ (whether or)
- [ ] 〜であれ〜であれ (whether or)
- [ ] 〜であろうと〜であろうと (whether or)
- [ ] 〜ようが〜まいが (whether or not)
- [ ] 〜ようと〜まいと (whether or not)
- [ ] 〜ようが / 〜ようと (even if)
- [ ] 〜ようにも〜ない (even if one tries, cannot)
- [ ] 〜ところを (although or when)
- [ ] 〜ところに / 〜ところへ (just when)
- [ ] 〜ところで (even if)
- [ ] 〜たところで (even if)
- [ ] 〜ところだった (nearly happened)
- [ ] 〜ところを見ると (judging from)
- [ ] 〜ところによると (according to)
- [ ] 〜ものを (although or if only)
- [ ] 〜ものとして (assuming or treating as)
- [ ] 〜ものとする (shall or assume)
- [ ] 〜ものと見られる (is considered)
- [ ] 〜ものと思われる (is thought)
- [ ] 〜とあって (because circumstances are)
- [ ] 〜とあれば (if the circumstances call for it)
- [ ] 〜とあっては (if circumstances are such)
- [ ] 〜とあっても (even if)
- [ ] 〜からして (judging from or even)
- [ ] 〜からすると / 〜からすれば (from the standpoint of)
- [ ] 〜から見ると / 〜から見れば (from the viewpoint of)
- [ ] 〜から言うと / 〜から言えば (from the viewpoint of)
- [ ] 〜からといって (just because)
- [ ] 〜からある / 〜からの (as many as)
- [ ] 〜からなる (consist of)
- [ ] 〜までして (going so far as to)
- [ ] 〜までのことだ (only have to)
- [ ] 〜までだ (nothing more than)
- [ ] 〜までもない (no need to)
- [ ] 〜までもなく (without needing to)
- [ ] 〜にして (at or only after)
- [ ] 〜にして初めて (only after)
- [ ] 〜にしてはじめて (only after)
- [ ] 〜にしてもなお (even so)
- [ ] 〜にしたところで (even if)
- [ ] 〜としたところで (even if)
- [ ] 〜にしたら / 〜にすれば (from someone's perspective)
- [ ] 〜にしてからが (even)
- [ ] 〜にしてみれば (from someone's perspective)
- [ ] 〜にしてみると (from someone's perspective)
- [ ] 〜ではあるが (although)
- [ ] 〜である以上 (given that)
- [ ] 〜であるからには (given that)
- [ ] 〜でなくて何だろう (what else could it be)
- [ ] 〜と言わんばかりに (as if saying)
- [ ] 〜と言っても過言ではない (it is no exaggeration)
- [ ] 〜といったところだ (approximately)
- [ ] 〜といったらない (indescribably)
- [ ] 〜といったら / 〜といえば (speaking of or exclamation)
- [ ] 〜というと / 〜といえば (speaking of)
- [ ] 〜といった (such as)
- [ ] 〜というものではない (not necessarily)
- [ ] 〜というものでもない (not necessarily)
- [ ] 〜というものがある (something called)
- [ ] 〜というものの (although)
- [ ] 〜というわけだ (that is why)
- [ ] 〜というわけではない (not necessarily)
- [ ] 〜といって差し支えない (fair to say)
- [ ] 〜といえよう (one could say)
- [ ] 〜と言わざるを得ない (must say)
- [ ] 〜と言うまでもない (needless to say)
- [ ] 〜に言わせれば (in someone's opinion)
- [ ] 〜に言わせると (according to someone)
- [ ] 〜と言うよりほかない (can only say)
- [ ] 〜と見える (appears that)
- [ ] 〜とみられる (is considered)
- [ ] 〜とされる (is regarded as)
- [ ] 〜とされている (is regarded as)
- [ ] 〜と思いきや (contrary to expectation)
- [ ] 〜と思ったら (when one thought)
- [ ] 〜と思うと (when thinking of)
- [ ] 〜と思われる (is thought)
- [ ] 〜と考えられる (can be considered)
- [ ] 〜と考えると (considering)
- [ ] 〜といっては (whenever saying)
- [ ] 〜かと思えば (then unexpectedly)
- [ ] 〜かと思いきや (contrary to expectation)
- [ ] 〜かと思えば〜 (alternating situations)
- [ ] 〜か〜か (whether or)
- [ ] 〜か否か (whether or not)
- [ ] 〜かのようだ (as if)
- [ ] 〜かのように (as if)
- [ ] 〜かのごとく (as if; literary)
- [ ] 〜がごとく / 〜がごとき (like; literary)
- [ ] 〜ごとに (each or every)
- [ ] 〜ごとく (like; literary)
- [ ] 〜にあたる (corresponds to)
- [ ] 〜に当たって (on the occasion of)
- [ ] 〜に当たり (on the occasion of; formal)
- [ ] 〜に際し (on the occasion of; formal)
- [ ] 〜に際しては (on the occasion of)
- [ ] 〜に際しての (in connection with)
- [ ] 〜に向けて (toward a goal)
- [ ] 〜に向けた (aimed at)
- [ ] 〜に向かう (heading toward)
- [ ] 〜に向かって (toward)
- [ ] 〜をめどに (aiming for a target)
- [ ] 〜をもってして (by means of)
- [ ] 〜をもってすれば (given the ability of)
- [ ] 〜をもちまして (as of; polite)
- [ ] 〜をもって代える (substitute as)
- [ ] 〜にとどまらず (not limited to)
- [ ] 〜にとどまる (remain limited to)
- [ ] 〜にとどまらない (extend beyond)
- [ ] 〜に限ったことではない (not limited to)
- [ ] 〜に限り (only; formal)
- [ ] 〜限りでは (as far as)
- [ ] 〜限り (as long as or to the extent)
- [ ] 〜ない限り (unless)
- [ ] 〜限りにおいて (insofar as)
- [ ] 〜限りの (as much as possible)
- [ ] 〜を限りにして (ending with)
- [ ] 〜に限っては (only in the case of)
- [ ] 〜に限って言えば (speaking only of)
- [ ] 〜に限らず〜も (not only but also)
- [ ] 〜を問わず / 〜を問わない (regardless of)
- [ ] 〜を問うことなく (without regard to)
- [ ] 〜のいかんにかかわらず (regardless of)
- [ ] 〜に比する (compare with; formal)
- [ ] 〜にも増して (even more than)
- [ ] 〜に反して (contrary to)
- [ ] 〜に反する (contrary to)
- [ ] 〜とは反対に (contrary to)
- [ ] 〜に反し (contrary to; formal)
- [ ] 〜に代えて (in place of)
- [ ] 〜に代わる (replacing)
- [ ] 〜に替えて (in exchange for)
- [ ] 〜にかわって (on behalf of)
- [ ] 〜に代わり (on behalf of; formal)
- [ ] 〜に伴い (accompanying)
- [ ] 〜に伴う (associated with)
- [ ] 〜につき (per or because of; formal)
- [ ] 〜につきましては (regarding; polite)
- [ ] 〜につけては (concerning)
- [ ] 〜に関し (concerning; formal)
- [ ] 〜に関しては (concerning)
- [ ] 〜に関しての (concerning)
- [ ] 〜に対し (toward or whereas)
- [ ] 〜に対する (toward)
- [ ] 〜に対しては (toward or in contrast to)
- [ ] 〜に応じ (depending on)
- [ ] 〜に応じた (suited to)
- [ ] 〜にしたがい (according to or as)
- [ ] 〜に従う (follow)
- [ ] 〜に連れて (accompanying; distinguish lexical uses)
- [ ] 〜に従って (in accordance with)
- [ ] 〜に応えて (in response to)
- [ ] 〜に応える (meet expectations)
- [ ] 〜にこたえて (in response to)
- [ ] 〜にかかわって (involving)
- [ ] 〜に関わりなく (regardless of)
- [ ] 〜にかかわりなく (regardless of)
- [ ] 〜にかまわず (regardless of)
- [ ] 〜を構わず (regardless of)
- [ ] 〜を押して (despite)
- [ ] 〜を押し切って (overriding objections)
- [ ] 〜を振り切って (shaking off)
- [ ] 〜を尻目に (with indifference to)
- [ ] 〜をものともせずに (undeterred by)
- [ ] 〜を顧みることなく (without regard to)
- [ ] 〜を抜きにしては (without, cannot)
- [ ] 〜を抜きにしても (even without)
- [ ] 〜抜きでは (without)
- [ ] 〜なしでは (without)
- [ ] 〜なしには (without)
- [ ] 〜なしで済む (manage without)
- [ ] 〜なくて済む (avoid having to)
- [ ] 〜ないで済む (avoid having to)

## Reference and scope

[OpenJLPT](https://github.com/evanclan/OpenJLPT) reports 526 grammar points
and supplies a useful cross-check; its levels are approximate. This checklist
is project-authored and is not an import of its descriptions or examples.
It includes ordinary grammar, expressions and colloquial forms beyond JLPT
coverage. Some entries overlap or need verification and consolidation; their
presence is a lead for investigation, not proof that a separate card is useful.
Prioritize real source occurrences and assess ambiguity and recall before
implementing each rule.

