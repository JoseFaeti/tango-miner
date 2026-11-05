# Shared grammar inflection improvements

[Grammar implementation queue](next.md)

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
   Sudachi’s `なくっ` + `た` + `ってな` analysis remains unsupported.
   A same-input run found `〜ていく` down two and lexical `いく` and
   `ない` each up two; the Anki preview had no deletions. Other effects on
   expression precedence and recall still need investigation.
2. [~] Extend positive verb inflection handling for observed stem and
   tokenization patterns used by `〜ていく`, `〜てくる` and `〜てしまう`.
   These rules already cover many common past and polite forms; moving existing
   logic alone is a refactor, not evidence of improved coverage.
   `〜てしまって`, `〜ちゃって` and `〜じゃって` now include an
   adjacent separate connective `て`. A raw replay of one diagnostic archive
   found 1,049 such matches, up from zero. A same-input run increased the
   grammar frequency by 1,049, with corresponding decreases in `しまう`,
   `ちゃう` and `じゃう`. `ても`, `ては` and `てもいい` also fell;
   overlapping expression reservation needs investigation. The Anki preview
   reported no deletions.
   Fused `てる` tokenizations and other positive forms remain future work.
   The shared positive matcher also supports caller-selected connective and
   conditional stems. `〜てみる` and the benefactive family use these for
   selected `て` and `ば` forms and recognize selected volitionals; `〜ていく`
   and `〜てくる` use it for selected conditional forms. `〜てしまう`
   also recognizes selected conditional and volitional split and contracted
   forms. Focused Sudachi
   tests cover complete spans and lexical controls. Corpus effects, overlapping
   grammar and expression precedence have not been evaluated for these additions.
3. [~] Consolidate and extend polite-suffix handling for supported `ます`,
   `ました`, `ません` and `ませんでした` tokenizations. A shared matcher
   handles selected split and whole-suffix analyses, with callers choosing
   positive, negative, connective or volitional forms. It rejects incomplete stems and
   incomplete `ませんでし`. In the first 20,000 archived sentences, raw rule
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


## Shared matcher design

The shared `positive_verb_end` helper recognizes
only explicitly supplied verb forms; each rule must choose its own supported
inflections and semantic constraints. The shared `negative_predicate_end`
helper supports selected plain and polite negative forms. The shared negative
suffix matcher is also used by verbal negation, desire, explanatory, aspect,
direction and completion rules. Rules explicitly choose supported forms and
continue to handle their own contractions and meanings. These rules retain
previously recognized conditional, colloquial and dialectal negative token
boundaries, including some partial spans; they do not claim full construction
coverage or resolve lexical expression ambiguity.
