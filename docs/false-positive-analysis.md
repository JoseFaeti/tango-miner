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
distinct component sequences, including unmerged tokens. This evidence is not
yet displayed or used for candidate typing or definition selection. It does
not change eligibility, merges, or output.

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
