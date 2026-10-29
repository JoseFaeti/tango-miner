# Grammar shadow prototype

The opt-in pipeline mode writes `grammar-shadow.jsonl` in the run archive,
after raw tokenization and before dictionary merging:

    python -X utf8 tango_miner.py --input path/to/corpus --output path/to/output.csv --grammar-shadow

The normal configured Anki behavior still applies; disable Anki explicitly in
the configuration before using this for a safe corpus check. The separate
grammar card stage is enabled by default; pass `--no-grammar` to disable it.
The shadow stage
does not change token streams, candidates, CSVs, or Anki plans. Output contains
normalized sentences, raw token evidence and source paths. Original text and
source offsets are not yet carried through normalization, so raw spans are null.
Sentence IDs in this pipeline mode depend on source path, sentence index and
normalized text, and can change when input ordering changes.

An offline mode reads samples and analyzer JSONL from the existing analyzer comparison:

    python -X utf8 -m src.GrammarShadowCLI --sample checks/raw.jsonl --analysis checks/sudachi.jsonl --output checks/grammar-shadow.jsonl

Create the output directory first; the output is created exclusively. Rows contain source text, aligned analyzer tokens and tentative proposals. Despite the normalized_sentence field name, this CLI passes sample text unchanged and records identity raw spans. These offsets apply to the raw sample line, not a pipeline-normalized sentence. Samples and evidence may be private.

The initial catalog detects literal んだ, ている, でいる, てる, でる and ものの. A narrow token-based rule also proposes inflected ている and contracted forms such as でいた and てた when the observed tokens meet its constraints. Every match is ambiguous, including fragments and unrelated lexical uses. This is a discovery baseline, not a validated grammar classifier. It can miss inflected and other variants. The evidence records whether boundaries match and whether the token analysis contains a nominalizer and copula, connective and いる, or particular ものの analyses. This evidence is not confirmation. Alignment failures produce no proposals. In offline mode sentence and proposal IDs depend on the sample ID and text, not sample order; changing either changes the IDs. The rule file hash accompanies successful rows, and offline rows also include analyzer metadata.

A targeted six-line sample yielded two proposals; a 69-line raw sample yielded six. Neither result measures correctness or recall. No external grammar data has been imported or pinned. OpenJLPT is a potential reference; inspect and pin exact entries, verify quality and comply with its CC BY-SA 4.0 notice before reuse. The prototype records general reference links only.

In the 69-line Sudachi sample, version 2 reports four connective-plus-いる
analyses, one んだ with matching token boundaries, and one ている-family
match inside a token; no alignment errors were reported. These are analyzer
observations, not independently reviewed correctness judgments.

A later 25-sentence synthetic pilot ran with Anki disabled and a workspace-local
cache. Baseline and shadow runs produced byte-identical main, dropped and
sentence CSV files; the shadow artifact recorded 25 tentative proposals and no
alignment errors. These repeated, selected examples check integration and do
not establish general accuracy or recall. Real `死んだ` tokenization also
produced a tentative `んだ` match; it must remain for review, not become an
explanatory definition automatically. Token analyses identified past and polite
`〜ている` and one contracted past form in a small hand-selected probe.

OpenJLPT's `te-iru`, `mono-no`, and `n-desu-no-desu` entries are relevant
references for the pilot forms. Their human-readable formations are not
executable rules; no entry text was imported. The links in output follow the
upstream main branch and are not pinned data. Before importing explanations or
using them for automatic definitions, pin and review the source and satisfy its
license and attribution requirements.

In the full-corpus shadow run dated 2026-10-28, 521,821 normalized sentences
produced 76,854 tentative proposals with no alignment failures. The JSONL was
approximately 1.19 GB, and the shadow stage took 34.6 seconds. This run used
Anki preview mode; it did not apply its proposed note changes. Inspection
found incomplete polite-form surfaces such as `ていまし` and `てませ`.
Version 4 extends only adjacent, token-supported endings, for example to
`ていました` or `ていません`; unresolved tokenizations remain tentative.
Replaying 810 occurrences that had these partial surfaces from the archive
showed the majority extending to longer surfaces. This is a targeted diagnostic,
not an independent accuracy or recall assessment; no corpus rerun has yet
measured the revised rules. Raw-source spans and a compact archive format remain
open work.

Remaining work includes original-to-normalized source mapping, broader
component-aware rules, merged-token evidence and independent review. Standalone
proper nouns remain filtered by the existing pipeline. This experimental layer
does not affect CSV or Anki.

## Grammar cards

The shared grammar stage runs by default. Pass `--no-grammar` to disable it.
The stage checks the registered rules and adds separate
grammar candidates to
the normal CSV and, when Anki is enabled, to its usual plan. Only uninflected
source occurrences with adjacent Sudachi continuative verb/auxiliary + connective て/で +
non-independent `いる`, or a contracted auxiliary `てる`/`でる`, are counted.
Supported forms include plain, past, negative, polite and some contracted
inflections, such as `でいた`, `ていなかった`, `ていませんでした`,
`てる` and `でた`. Contracted negatives include analyses such as `でない`,
`でなかった`, `でなく` and `でませんでした`, even when Sudachi merges
or mislabels their components. These surface and context cues can still be
ambiguous; other tokenizations remain unsupported. The
example must also pass the existing sentence-validity check. No card is emitted
without an eligible example. Its frequency counts only the supported subset,
and its index is the mean first occurrence position in each source, measured
against eligible merged tokens. Grammar cards are added after lexical filtering
and definition lookup but before scoring, so they share frequency, position,
source diversity and normalization calculations with vocabulary. The grammar
label does not count as a source tag. The minimum-frequency threshold does not
apply to these grammar cards. Their source examples receive the same mean
recognized-word score as lexical examples; an example remains at zero when
its source sentence cannot be matched or contains no scored vocabulary. Before reserving grammar tokens, the established dictionary merger checks for
a complete source-matched expression spanning the construction and its attached
word. If it finds one, that occurrence is counted as the lexical expression
instead of grammar. This is a heuristic: dictionary matches can be ambiguous.
Remaining matched grammar tokens are reserved before lexical merging and collection.
The attached verb outside the highlighted span remains eligible. Other occurrences
of the same word remain lexical candidates. A rule reserves tokens only if it
has an eligible example for a card, and only at complete token boundaries.
The merger retains reserved neighboring tokens as context for inflection guards.
These decisions change lexical frequencies, examples and potentially output status; overlapping grammar rules may still match
the same tokens. This is a narrow evidence rule, not a guarantee
that every occurrence has a single unambiguous aspectual meaning.

A replay of the first 20,000 sentences of an earlier shadow archive found
214 `〜てくる` matches and 206 reserved raw `くる` tokens, alongside
1,627 `〜ている` and 1,043 `〜んだ` matches. These are ordered,
source-biased raw-token counts, not post-merge frequency changes or a precision
and recall evaluation. Treating a match exclusively as grammar can remove a
useful lexical analysis when the rule is wrong; unmatched uses remain eligible.

For explanatory `んだ`, a dictionary merge beginning at copular `な`
does not override the supported `な` + `ん` + copula analysis. This avoids
interpreting `だめなんだって` as interrogative `なんだって`. Longer
merged expressions such as `そうなんですか` may still take precedence,
and other ambiguous tokenizations remain unresolved by this rule. An ordered
20,000-sentence archive replay found 218 copular matches; the former precedence
rule preempted 141, including longer expressions, so that number is not a
measurement of the narrowed rule or a representative accuracy estimate.

The `〜んだ` card covers explanatory んだ, a common conversational
construction. Supported adjacent token analyses include ん/の with だ or です,
past, conjectural and common negative forms, and のである. The written の
form requires a preceding predicate to limit possessive matches. The rule also
recognizes single-token のだ and のです after a predicate, and a split の +
だった analysis. Highlights include the recognized ending. This is a
morphological cue, not a guarantee of the intended nuance: nominalized things
and some negative constructions can still be ambiguous. Other tokenizations,
including some where the nominalizer is merged with the preceding word, bare
explanatory questions, and unaligned streams can be missed. As with 〜ている,
a valid source example is required; other occurrences remain available as lexical candidates.
Existing notes with this exact identity are protected from automatic updates
and pruning, so their examples do not refresh automatically. Corpus diagnostics
recorded 10,894 split んだ analyses, but they were not independently evaluated
as grammar matches and do not measure this expanded rule's precision or recall.

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
but existing Anki grammar notes retain their prior examples and frequency.
The later contracted-negative extension found six `でない` and one
`でなかった` in the same 20,000-sentence replay. The observed examples
support those matches, but this small, ordered check does not establish
general precision or recall.

The `〜たい` card expresses a desire to perform an action. It
requires a source-aligned continuative verb followed by Sudachi's `たい`
auxiliary. It recognizes plain, past, negative, conditional and selected polite
forms, such as `たい`, `たかった`, `たくない`, `たければ` and `たいです`.
It also recognizes continuative `たく`, for example in `たくなる`. The
distinct `たがる` construction is not included. Some valid forms are missed
when the analyzer merges the verb and auxiliary, and morphological evidence
does not guarantee the intended interpretation. Other occurrences remain
available as lexical candidates and existing notes with this exact identity are protected like the
other grammar notes. An ordered replay of the first 20,000 archived sentences
found 343 matches; this is not an independent precision or recall evaluation.

The `〜てくる` card describes movement toward a point, an action
followed by return, or a change emerging or continuing up to the present,
depending on context. It requires an adjacent continuative verb or auxiliary,
connective て/で and non-independent くる/来る. Supported split forms include
plain, past, negative, polite, て-form and volitional forms. It does not
recognize merged words such as `持ってくる` or ambiguous `てく`, which can
also represent `ていく`. Morphology alone cannot distinguish the meanings.
Other occurrences remain available as lexical candidates and existing notes with this identity are
protected like the other grammar notes.

The `〜ものの` card covers concessive “although ...” uses when
Sudachi identifies a single `ものの` conjunction immediately after a verb,
adjective, or auxiliary in terminal or attributive form, and text follows it.
Split nominal `もの` + `の` is not matched. This narrow analysis can miss
valid constructions and does not prove that the following text is a complete
contrast; matching tokens are reserved from lexical processing. Existing notes with this
identity receive the same protection as the other grammar cards.

The `〜てしまう` card describes completion or an unintended or
regrettable result, depending on context. The rule recognizes an adjacent
continuative verb plus connective て/で and non-independent しまう, as well
as the auxiliary contractions ちゃう/じゃう. Supported forms include plain,
past, common negative and polite inflections. Matched grammar tokens are excluded from lexical processing; the grammar card requires an eligible source example and
its Anki identity is protected from updates and pruning. Other tokenizations,
inflections and nuances may be missed, and the morphology alone cannot decide
the intended nuance. In an ordered replay of the first 20,000 archived
sentences the rule found 197 occurrences, including 77 `ちゃった` and 75
`ちゃう`. This limited, selected check does not establish precision or recall.

The distinct Expression is the Anki identity. A new card participates in the
existing preview and confirmation flow. If a note with that exact expression
already exists in the configured deck/model, its fields and other tags are preserved;
its frequency and examples do not refresh automatically. When the card is present
in the current output, the plan removes the old synthetic `grammar` tag unless
`grammar` is an actual input source tag. New cards receive only source tags. The plan lists it under `retained_existing_grammar`.
The identity is also protected from pruning in later runs where the stage is
disabled or no occurrence is found. With pruning enabled, a lexical note that disappears from the output can be
planned for deletion; review the entire plan before approving it. Grammar settings
do not enable Anki or disable other planned updates. Review the complete Anki plan before applying it.

Grammar identities use `〜` as an attachment marker, for example `〜ている`.
The marker does not guarantee uniqueness against vocabulary expressions; the
pipeline rejects collisions. For an existing suffix-style note, an Anki plan
renames its Expression in place when the current card exists, there is exactly
one old note, and no note already has the new identity. Other fields, custom
tags and scheduling remain intact; the existing synthetic-tag cleanup applies.
When both identities exist or multiple old notes share an identity, the plan
records `grammar_migration_conflicts` and leaves them untouched. Both old and
new identities are protected from pruning, including when grammar is disabled.
Plans still require the normal approval before any Anki mutation.
The shared grammar stage collects all registered rules in one pass; the output
card stage adds its cards before scoring. Each matcher and its metadata live in a separate module under `src/grammar_rules/rules/`;
`registry.py` lists the rules used by the shared stage. The corresponding
`tests/test_grammar_*.py` files check each rule. When adding a rule, register it
and its identity in the Anki protection set; startup validates that both
registrations match.

For a bounded proposal-review sample from an existing shadow archive:

    python -X utf8 -m src.GrammarReview --input runs/example-run/grammar-shadow.jsonl --output checks/grammar-review.jsonl --per-stratum 5

The sampler streams the archive and deterministically selects up to the limit
per construction and evidence combination. Rows include stratum population,
source text, tokens, proposal, and empty human-review fields. They are
proposal-selected and stratified, not a representative sentence sample; they
cannot measure missed grammar or corpus-wide accuracy. Their contents may be
private. Do not use the same examples for both rule design and independent
evaluation.

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
