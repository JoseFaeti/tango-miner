# False-positive analysis: suspended Anki candidates

Date: 2026-10-26

## Project and documentation policy

The project mines Japanese from material the learner intends to read. Its
priority is **high recall**: keep useful vocabulary, grammar, fixed
expressions, colloquialisms, and uncertain candidates rather than risk losing
learning material. “False positive” in this document means a candidate whose
target or definition is demonstrably misleading; it does not mean that every
non-lexical or multi-token expression should be excluded.

Suspicious candidates are review signals and regression-test seeds, not a
blacklist or an automatic removal queue. Future pipeline changes must preserve
useful candidates by default and use confidence/provenance to improve their
target form, examples, or definitions.

Documentation must not contain absolute local paths or other personal machine
details. Refer to workspace-relative paths or describe supplied files
generically.

## Scope

This analysis reviews `resources/suspended (false positives).txt`, an export of
vocabulary candidates suspended from the Anki deck. It is intended to guide
improvements to the mining pipeline rather than to serve as a permanent
blacklist.

## Observations

The export contains 178 suspended candidates.

- 133 candidates (about 75%) are kana-only.
- 26 candidates are a single character.
- Several clear false positives are very frequent: `てく` (1,033), `ないし`
  (535), and `しれる` (323).

Consequently, increasing the minimum-frequency threshold is not an effective
fix. These errors are systematic tokenization or dictionary-selection errors,
not rare noise.

Representative examples:

| Candidate | Sentence use | Bad analysis |
| --- | --- | --- |
| `しよう` | `何をしようが` | Noun “use” rather than the verbal construction |
| `さい` | `さいだん` / `さい月` | “years old” instead of the start of a larger word |
| `てく` | `ついてく` | Auxiliary/dependent morphology emitted as vocab |
| `ないし` | `できないし` | Dictionary conjunction rather than `ない` + `し` |
| `ようやく` | “finally” | Incorrectly defined as “summary” |
| `風花` | Persona character name | Incorrectly defined as “flurry of snow” |

## Recommended improvements, in priority order

### 1. Use kana-only POS analysis as contextual evidence

Treat kana-only POS analysis as a confidence signal. Preserve useful kana-only
lexical items, grammar, and expressions unless there is strong evidence that a
candidate target is wrong.

The suspension export showed many ambiguous kana forms such as `しよう`, `さい`,
`ひょう`, and `てく`, but the recall-first policy means they should inform
target-selection improvements rather than justify broad exclusion.

**Status (2026-10-26): reverted for recall.** Kana-only auxiliaries,
non-independent forms, and short kana-only nouns remain eligible. The pipeline
now favors preserving candidates and improving an incorrect target selection
later, rather than hiding potentially useful grammar or colloquial material.

### 2. Recover kana-written compounds without inventing a target

Game-script text often writes ordinary words in kana. For example, `さいだん`
can be split into `さい` and `だん`; emitting the first fragment is not useful.

Allow a multi-token kana sequence to become a candidate only when its target is
grounded in the source surface. If a reliable compound cannot be recovered,
retain the candidate but mark it as ambiguous rather than suppressing it.

**Status (2026-10-26): partially implemented.** Dictionary compound lookup now
uses the exact combined source surface rather than concatenated token lemmas.
This prevents unrelated targets such as `おまつ` from polite `おまち` and
`ぬし` from `んし`, while preserving the source-derived candidate. A narrow
noun-plus-suffix reconstruction is described below; broader compound
reconstruction remains future work. Component provenance is now
retained as described below; it does not change merge decisions.

### 3. Preserve auxiliary and dependent morphology with candidate-type context

Grammar forms such as `てく`, `ないで`, `られる`, and `ちゃう` are in scope.
Their POS should be preserved as candidate-type context so that definitions and
presentation can improve without filtering them out.

**Status (2026-10-27): partial.** Merged tokens retain immutable component
surfaces, lemmas, base forms, readings, and POS. Candidate aggregation counts
distinct component sequences, including unmerged tokens. Some definition and
parsing corrections use these counts. Optional occurrence diagnostics now
export the individual source-linked analyses and final token rule, including
tokens excluded before aggregation; candidate diagnostics provide final status
and definition. Optional diagnostics also record rejected dictionary matches considered by the greedy merger and the selected JMdict entry when available. These records do not change candidate eligibility; skipped alternatives and derived potential definitions are not represented as selected entries.

### 4. Choose definitions using POS, reading, and source form

Dictionary selection currently favours JMdict frequency metadata without enough
evidence from the token's POS, source spelling, inflection, and context. This
produces definitions that visibly disagree with their examples, including
`ようやく`, `好く`, and `きげん`.

Prefer an exact spelling match and compatible POS before applying frequency
priorities. Retain the token reading and surface form through lookup so that
homophones can be distinguished.

### 5. Use single-kanji candidates as a confidence signal

Single-kanji candidates can be fragments, verb stems, or pieces of a larger
kana-written word, but common standalone words such as `手` and `目` are useful.
Keep them and use surface/POS/reading agreement as a future confidence signal,
not as an exclusion criterion.

### 6. Preserve proper-noun evidence through token merging

Raw token analysis can identify proper nouns, but merging may replace that
information with a generic dictionary entry. Preserve this evidence as context
for target and definition selection. Names and game-specific terms remain in
scope because they occur in the reading material; do not exclude them merely
because they are proper nouns.

**Current limitation (2026-10-27):** merge components retain raw proper-noun
POS, but the existing token filter excludes standalone `名詞` / `固有名詞`
tokens before candidate aggregation. A merged candidate can retain that
evidence if it passes the existing filter. Provenance does not reverse the
filter or make names eligible; aligning that behavior with the recall-first
policy remains future work requiring focused regression tests.

### 7. Use a confidence score instead of only binary filters

Combine existing signals into a candidate-confidence score:

- POS reliability
- kana-only and word-length penalties
- agreement between source surface and lemma
- dictionary/POS/reading agreement
- confidence in compound reconstruction
- consistency across example contexts

Candidates below a threshold should remain available in normal output, with a
confidence marker or review tag where practical.

### 8. Turn suspended cards into a regression corpus

Use representative suspended entries and audited output cases as tests, rather
than relying on a long-lived blacklist. Current coverage includes `しよう`,
`さい`, `てく`, `ようやく`, grammar expressions such as `ないで` and `ないと`,
colloquialisms such as `かっけえ`, and the prior unrelated-target failures
`おまつ` and `ぬし`. Add `ないし` and `風花` when their intended target/display
behavior is defined.

## Suggested implementation sequence

1. **Completed baseline:** retain audited regression coverage for recall,
   grammar, colloquialisms, and source-surface target selection.
2. **Completed foundation:** preserve raw component provenance through merging
   and aggregate distinct analyses and counts for eligible candidates.
3. Investigate the existing proper-noun exclusion with recall regressions, and
   define intended name handling before changing it.
4. Use provenance to improve contextual dictionary-entry selection and
   definition handling; expose candidate-type context where useful.
5. Add confidence labels without removing uncertain learning material.

## Relevant code

- `src/steps/TokenizeStep.py`: initial token-level filtering.
- `src/steps/MergeTokensStep.py`: dictionary-based compound merging.
- `src/TokenProvenance.py`: immutable raw component evidence.
- `src/WordStats.py`: counts of component sequences per eligible candidate.
- `src/JMDict.py`: entry selection and definition construction.
- `src/steps/BuildWordStatsStep.py`: candidate aggregation.
- `src/steps/FilterFrequencyStep.py`: frequency filtering; this should not be
  treated as the primary false-positive control.

See also [the initial output audit](anki-output-audit-2026-10-26.md) and the
[18,855-entry re-audit](anki-output-reaudit-2026-10-26.md).

## Targeted grammatical-boundary fixes (2026-10-27)

The supplied examples include `できないし` and `そんなことしてみろ`:
matching their surfaces across Sudachi tokens previously produced the unrelated
dictionary entries `ないし` (“or”) and `ことし` (“this year”). The merge
stage now rejects a conjunction-only entry spanning an auxiliary and particle,
and a noun/adverb-only entry spanning a noun and an inflected `する` stem.
The original tokens remain available; genuine standalone `ないし` and
`ことし`, and auxiliary expressions such as `ないで`, remain covered by tests.
This is a targeted improvement, not a guarantee that all suspended examples
disappear: definition ambiguity, proper names, and incorrect single-token
Sudachi analyses remain unresolved. Mixed uses of a
word may still aggregate into one card.

## Kana noun reconstruction (2026-10-27)

The merge step also recognizes an exact hiragana JMdict noun surface when
Sudachi splits it into an unchanged common noun and an unchanged noun-like
suffix. This narrowly recovers `さいだん` in both supplied examples, instead
of emitting `さい` from those occurrences. Components remain in provenance.
The rule does not reconstruct mixed-script `さい月` or split words with other
POS patterns, and a dictionary spelling alone cannot prove the intended word.

A before/after comparison tokenized all 363 example sentences in the supplied
suspension export. Eleven token streams changed: two `さいだん` examples,
`はんてい`, two `ばあさん` examples, `ばあちゃん`, `みなさん`,
`おっさん`, `じいちゃん`, and two `じょうちゃん` examples. In the
`ばあちゃん` sentence this also replaces a conflicting `ちゃんと` merge
with `ばあちゃん` followed by `と`. Regression tests cover every changed
sentence, component evidence, grammar boundaries, and restrictive conditions.
The comparison checks the effects of this rule; it does not establish that all
remaining analyses are correct, or measure impact on the full source corpus.

## Unresolved definitions and Anki preservation

A missing definition is review status rather than evidence that a candidate is
invalid. Such candidates remain in the existing `.dropped.csv` review output
and their sentence rows carry `review` status. Existing Anki notes for them are
protected from deletion and are not changed; new unresolved candidates are not
exported. Truly absent filtered candidates remain eligible for the existing
Anki deletion plan.

When a short kana lookup would be rejected as a noun, unanimous unmerged
auxiliary or pre-noun component POS can select a single compatible dictionary
entry. Nouns and other POS,
mixed analyses, merged components, unsupported POS, and multiple compatible entries stay in
review; POS alone cannot safely disambiguate noun homophones such as `そば`.
Source-grounded potential forms can receive a labeled definition from a
matching base verb and reading when the full potential form occurs. Other
inflections and ambiguous definitions
remain for review.

When all retained analyses consist of one adjective token with the same
dictionary form and reading, an incompatible frequency-selected entry can be
replaced by a unique JMdict adjective entry matching that reading. For example,
`ふかい` can select “deep” instead of the noun “discomfort”. Mixed or merged
analyses, incompatible readings, multiple matching entries, and already
compatible entries keep the previous selection. This is not a guarantee that
Sudachi's POS is correct; ambiguous existing selections are not automatically
marked for review. Adverbs remain unchanged: corpus inspection showed
misanalysed word fragments and a noun use of `どかん` tagged as adverbs.

Definition output now respects JMdict sense restrictions for the spelling or
reading being looked up. A restriction on the other dimension is retained
because that dimension is not established by the lookup alone. If the chosen
entry has no applicable gloss, the candidate remains available for review;
selection does not yet search alternative entries using these restrictions.

## Grammatical occurrence corrections

The conjunction merge guard covers `ない` tagged as an adjective followed by a
particle, as well as the pre-existing auxiliary boundary. A single Sudachi
`ことし` token followed by certain `する` inflections is reconstructed as
`こと` + `し`, retaining the original token as evidence. The genuine
`ことし` candidate and `ないし` conjunction remain eligible. A single-token
`ないし` such as `自信ないし` is not reconstructed by this rule.

Clearly delimited godan imperatives that Sudachi labels as potential lemmas
are reassigned to their verified base verbs. This does not handle every
imperative or resolve uncertain sentence fragments. If reassignment would
otherwise push a real potential candidate below the frequency cutoff, the
number of reassigned occurrences can preserve its filter eligibility. They are
not included in the candidate's recorded frequency or example sentences,
and no potential candidate is created without a real occurrence. Existing
frequency and source-coverage thresholds otherwise remain in force.

The completed 2026-10-27 run also included colloquial `かもしれねえ` examples
under `しれる`. The merge stage now recognizes the four-token negative
construction before greedy merging, assigning its source surface to
`かもしれない` and retaining the original components. The matcher also handles
the verified negative forms `かもしれぬ`, `かもしれん`, and `かもしれませぬ`,
including a single-token `かもしれん`. It does not generalize arbitrary lemma
concatenations or normalize `やもしれん`. This also prevents a
preceding `じゃないか` merge from consuming its `か`. Ordinary `しれる` uses
and genuine `じゃないか` remain eligible. Other suspected occurrences in
that run, including `つかる` for `つかって`, need further contextual analysis;
the presence of a genuine use prevents treating the entire entry as invalid.

The `わる` card also included `気が変わったら` examples when Sudachi
analyzed `気が変` followed by `わっ`. A narrow boundary repair reconstructs
`気` + `が` + `変わっ` only with a matching following inflection and a
verified `変わる` token and dictionary entry. The original components are
retained; genuine `気が変` and `わる` remain eligible. A scan of cached raw
token streams for the last run found 19 affected streams, not necessarily 19
exported occurrences. Other forms and boundary errors remain unaddressed.

In a subsequent run, some recovered `気が変わった` predicates were merged
as the JMdict adjective `変わった` (“unusual”). The merger now keeps the
past verb when `気` + `が` precedes it and a clear sentence boundary or
following `か` / `ん` particle supports the predicate. It leaves adjectival
uses such as `変わった鉱物` eligible. A comparison of the last run's cached
raw streams found 13 affected streams; ambiguous other contexts are unchanged.

The same failure affects other verb-plus-`た` sequences. A general merge guard
now keeps the verb when Sudachi identifies a kanji-written verb and past
auxiliary, the selected JMdict entry is exclusively prenominal, and the next
token is a sentence-ending mark or the particle `か` / `ん`. For example,
`疲れた。` is assigned to `疲れる`, while `疲れた人` remains eligible as
`疲れた`. Commas, clipped sentence endings, kana-only verbs, and other POS
types remain unchanged because their interpretations can be ambiguous. This
does not correct an independently wrong Sudachi verb analysis.

Comparing merged streams with and without this guard over the latest run's
cached inputs changed 90 streams across ten surface forms. These include 33
`変わった`, 28 `疲れた`, and 10 `慣れた` streams; repeated source sentences
are included, and the comparison does not measure exported candidate counts.

## Yet-to-learn card review (2026-10-27)

The latest completed run's displayed examples show additional definition and
target-selection problems. `により` is defined as “similarity” where its
examples express means or accordance; `おって` is defined as a pursuer where
the examples use inflected auxiliary `おる` after a verb stem; and `覆われ`
is given a noun definition where the examples use passive `覆う`. The
`つれて` examples use `連れる`, not the proportional expression; two of
three `にかけて` examples use `掛ける`, while one is the swearing expression.
These examples motivate POS and inflection-aware selection as well as
occurrence-level expression disambiguation. Preserve genuine expression and
homophone uses when adding regressions; do not reject noun entries globally.

The same run defined `する` as “rub” or “squander” and `いる` as “need”,
although their displayed examples use “do” and auxiliary `いる`. These
high-frequency homophones are particularly valuable regression cases. The
review covers the exported examples, not all occurrences. The run did not
enable occurrence diagnostics, so its CSV does not establish the underlying
token analysis or the identity of the selected dictionary entry.


## Contextual homophone selection (2026-10-27)

For candidates with multiple dictionary entries, single-token analyses can
identify a unique entry by reading, POS, and available verb conjugation. The
most frequent supported entry supplies the definition; ties or analyses that
cannot distinguish entries remain for review. Examples are selected only from
occurrences supporting that entry, including the fallback path. Frequency and
score still count all candidate occurrences. Optional candidate diagnostics
report the supporting and other occurrence counts; occurrence diagnostics
retain the individual analyses. A non-independent verb can select a unique
entry with an auxiliary sense, but this is not full sentence-level semantic
analysis. Merged analyses and other unmatched occurrences are not assigned to
the selected definition. Single-entry and evidence-free lookups retain their
existing behavior. The output does not yet create separate cards or present
alternative definitions for other groups.

A scan of locally cached streams found mostly suru-verb analyses for `する`,
mostly ichidan non-independent analyses for `いる`, and mostly adverbs for
`ようやく`, alongside other or merged analyses for each. The scan covered
matching cached streams, not exactly the archived filtered occurrences.

A full-corpus check with Anki disabled used the same input manifest as the
latest archived run. Candidate counts and frequencies were unchanged. The
three target words received compatible definitions, while valid output fell
from 19,032 to 18,037 and review rose from 1,201 to 2,196 (a net 995 more
review candidates). This is a material change to normal output; the review
file preserves those candidates, but unresolved new candidates are not added
to Anki. This aggregate comparison does not establish whether each newly
unresolved analysis is genuinely ambiguous. Review and improve coverage before
considering the broader selection policy settled.


## Verb and inflection boundary repairs (2026-10-27)

The merger now leaves a verb and passive auxiliary separate when a noun-only
dictionary match spans them and following `た` or `て` confirms inflection.
For example, `覆われた` yields `覆う` and the auxiliary instead of the noun
`覆われ`. A continuative verb followed by non-independent `おる` + `て`
likewise retains `おる`, rather than merging the auxiliary as `おって`.
The guard also recognizes a continuative verb followed by connective `て` or
`で` and non-independent `おる`, as in `離れておって` and `読んでおって`.
It depends on those Sudachi analyses and does not repair cases tokenized into
other components. Standalone `おって` remains eligible.
The original components remain attached. Standalone noun and adverb uses are
not covered by these guards.

For a kana `つれる` stem followed by `て` and a motion verb, an object
particle in the nearby context supports the `連れる` target. Its original
token components and source surface are retained. Proportional `につれて`
and `つれて` without this cue remain eligible as before. The rule is not
a general solution to ambiguous expression boundaries, and other contexts
may still need review.

A full-corpus check with the same input manifest and Anki disabled moved
`覆う` from 44 to 184 occurrences and `おる` from 2,853 to 2,922; `連れる`
increased from 58 to 97. `つれてくる` remained in output at frequency 4.
`覆われ` and `雇われ` fell out of the filtered output after the boundary
repair; this comparison alone does not identify all remaining genuine noun
occurrences, so it does not prove there was no recall loss. `おって` still
has 47 review occurrences. The remaining `つれて` and `につれて` examples
still mix taking-along and proportional uses in some contexts; occurrence
disambiguation beyond the narrow cues above remains future work.

## Explanatory `んだ` (2026-10-28)

A diagnostic run recorded 10,894 `んだ` occurrences with nominalizing
particle `ん` plus auxiliary `だ`, alongside 8,594 single-token noun analyses
and smaller groups. The merger and dictionary selection now use that specific
split analysis to select the explanatory expression when exactly one JMdict
expression entry matches. Examples are restricted to supported analyses.
Single-token and other analyses remain unassigned; they include both plausible
explanatory uses and unrelated fragments, so they must not inherit the selected
meaning. Frequencies still count all occurrences and optional diagnostics report
supported and other counts. A valid output card does not provide a separate
review card for the other analyses; separate grammar and lexical identities
remain a design task. The corpus-wide result has not yet been rerun.


## Expression coverage and inflection (2026-10-28)

A targeted replay found seven `をめぐって` occurrences across seven sources
after grammar reservation. Some concern a topic, while others describe travel;
the JMdict expression definition does not fit all examples. Dictionary-backed
expressions meeting the frequency cutoff but failing per-source coverage are
now retained for review rather than discarded. They are not automatically added
to Anki, and existing notes are protected. This general review policy can retain
additional expressions. An Anki-disabled full-corpus comparison with the same
input manifest added 131 review candidates, removed none, and left valid output
unchanged in count; debug and Anki settings differed. This does not establish
the correctness of each added expression or definition.

For `目に見えている` and matched contracted variants, the source-aligned
`目に見え` components can form dictionary-backed `目に見える`, while the
following grammar ending remains reserved. Its dictionary entry includes both
visible and evident senses. A targeted replay found nine such occurrences in
seven sources. Three instances of `目に見えて` came from the same clipped sentence;
they do not establish the previous adverb definition. The inflected expression
is retained for review below the cutoff when its components support a sufficiently
frequent dictionary-backed base expression. This also protects an existing note
from pruning; it may retain other inflected expressions for review. Other spellings and tokenizations remain future
work. A subsequent Anki-disabled full-corpus run retained `をめぐって` for review
at frequency seven and `目に見えて` for review at frequency three. The existing
`目に見える` candidate rose from eight to 17 occurrences. The former examples
still mix meanings and the clipped adverb examples do not establish their
definition, so both original candidates remain review-only.

## Negative grammar and expression boundaries (2026-10-29)

A run introducing verbal `〜ない` removed nine previously valid expressions,
including `ないといけない` and `なくてもよい`; its Anki plan deleted their
notes. The expression-precedence check now also recognizes a longer dictionary
merge that starts at the grammar span. A targeted replay of one archived example
for each of the nine expressions preserved all nine with the configured
analyzer and dictionary. This is not a full-corpus comparison and does not
establish their final frequency or resolve any existing sense ambiguity. Existing
deleted notes require separate recovery; a new run alone cannot restore user edits.

## Conditional grammar and lexical boundaries (2026-10-29)

The first `〜たら` run introduced 13 misleading `気が変` and `わる`
occurrences where the source read `気が変わったら`, reducing `気` and
`変わる` by the same amount. Reserving `たら` had hidden it from the
existing boundary repair. That repair now uses a reserved ending as context
without changing reserved tokens. Targeted tests cover archived examples and
retain the source components; legitimate `気が変` and `わる` uses remain
covered separately. A full-corpus comparison has not yet verified the fix.

## Past-experience grammar boundary (2026-10-29)

The first `〜たことがある` run assigned `困ったことがあるなら` to
the experience card and reduced lexical `困ったこと` from 21 to 20. The
rule now leaves this conditional problem-expression context to lexical
processing, while retaining an immediately preceding `に` context and other
experience uses. This is a narrow ambiguity heuristic, not a general semantic
disambiguation; a corpus comparison has not yet verified its effect.

The follow-up run removed that example from the grammar card but did not
restore `困ったこと`: Sudachi merged `ことがある` into one token. A
targeted boundary repair now exposes `こと` for the dictionary merger in
the observed conditional and retains the original token evidence. A replay
with the configured tokenizer and dictionary recovered `困ったこと`. The
newly selected `言い忘れたことがあるわ` also describes an unsaid matter;
the grammar rule leaves this observed form lexical. These narrow changes need
a full-corpus comparison and do not resolve general nominal ambiguity.

## ておく and overlapping expressions (2026-10-29)

A same-input run introducing `〜ておく` removed `おけばいい` (18),
`おけばよかった` (14), and `従って` (4) from the prior output. The first
two are dictionary expressions starting at おく and extending beyond the
reserved grammar span. The grammar stage now gives such a longer JMdict
expression precedence, leaving its full token components for lexical merging.
Selected archived examples recover both expressions, while unrelated ておく
occurrences still produce grammar. A future same-input comparison is needed to
measure final frequencies and effects on other expressions.

The removed `従って` included `命令にだけ従っておけばいい`: there the
source supports the verb 従う, while the selected 従って definition meant
“therefore”. The merger now preserves 従う for this specific ておく context;
a standalone discourse use of 従って remains eligible. Other contexts may
still mix verb and adverb uses and require separate contextual analysis. These
changes have not yet been verified in a full-corpus run.
