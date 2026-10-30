# Anki output audit: 2026-10-26

## Input and method

Audited the supplied Anki output export, produced after the narrowed
kana-only candidate filter.

- Output rows inspected: **17,444**
- Rows with usable highlighted example sentences: **17,402**

For every usable row, each highlighted example span was re-tokenized with the
same Sudachi configuration used by the pipeline. This does not judge whether a
definition is correct. It checks whether the candidate is a plausible lexical
item in its displayed examples, and identifies cases in which the candidate is
manufactured from raw grammatical tokens or an apparent substring.

## Overall assessment

Most output rows are plausible vocabulary, inflected vocabulary, fixed
expressions, or useful grammar patterns. The dominant remaining technical
issue is the dictionary compound-merging stage: it can replace a sequence of
raw tokens with any JMdict match, losing the original POS evidence.

The project prioritizes **recall over precision**. Its purpose is to learn from
the material being read, so vocabulary, grammar, fixed expressions, and
colloquialisms are all in scope. The findings below are review signals and
regression-test candidates, not a removal list. A Japanese phrase can be useful
even when Sudachi segments it into several tokens.

## Suspicious-candidate review signals

### Function-only merge leakage (72 rows)

In every displayed example for these rows, the highlighted span consists only
of particles, auxiliaries, prefixes, suffixes, or punctuation according to the
raw Sudachi analysis. Many are meaningful grammar constructions; keep them in
the output for now. Their status is a signal that they need better provenance
and, eventually, grammar-aware definitions rather than that they should be
discarded.

High-frequency examples include:

| Candidate | Frequency | Raw components in examples |
| --- | ---: | --- |
| `んだ` | 16,220 | `ん` + copula |
| `たの` | 7,308 | past auxiliary + `の` |
| `んです` | 4,679 | `ん` + `です` |
| `では` | 3,416 | `で` + `は` |
| `ても` | 2,766 | `て` + `も` |
| `のだ` | 2,379 | `の` + copula |
| `ないで` | 1,572 | negative auxiliary + `で` |
| `ないと` | 1,569 | negative auxiliary + `と` |
| `ですか` | 658 | `です` + `か` |
| `ですから` | 590 | `です` + `から` |

Full inventory, ordered roughly by frequency:

`んだ`, `たの`, `んです`, `では`, `よな`, `のだ`, `ても`, `ないと`,
`ないで`, `ですが`, `ては`, `のです`, `てね`, `には`, `たって`,
`ですか`, `ですから`, `てよ`, `かの`, `おお`, `ずに`, `わい`, `てな`,
`じゃが`, `とは`, `だのに`, `までに`, `はよ`, `との`, `とさ`, `ぬし`,
`たた`, `にも`, `さも`, `つうか`, `ばかりに`, `さだ`, `よお`, `つーか`,
`よりも`, `なに`, `もと`, `まいか`, `かて`, `ですです`, `どっち`,
`っつうか`, `いっち`, `よりか`, `本目`, `での`, `ともども`, `っつか`,
`かいな`, `おっけ`, `さまで`, `ただに`, `おっぱ`, `如し`, `おも`,
`たりぃ`, `個目`, `にどと`, `だけしか`, `っちゅうか`, `っつーか`,
`まいよ`, `かっけえ`, `はやく`, `だりぃ`, `ねから`, `にて`.

Some items in this inventory are valid colloquial or grammatical expressions
(`かっけえ`, `だりぃ`, `ばかりに`, `までに`, etc.). They are in scope for this
project and should remain available to learn.

### Confirmed or strongly likely partial-word cards

These candidates do not consistently name the highlighted thing in their own
examples; they appear to be generated from a prefix, a conjugated stem, or an
unrelated homograph. Keep them for now under the recall-first policy, but use
them as high-value examples when improving merging. A later implementation
should prefer a more faithful target where possible, without deleting an
otherwise useful learning opportunity.

| Candidate | Frequency | Evidence from examples |
| --- | ---: | --- |
| `ぬし` | 47 | Highlight is `んし` in `ありませんし` / `開かんし`; dictionary word `主` is unrelated. |
| `おまつ` | 46 | Highlights `おまち` and `おまっ` from polite `待つ` forms; “black pine” is unrelated. |
| `おま` | 37 | Mixes the beginning of `おまさん` / `おまはん` with `おまちがい`; it is not a stable word. |
| `おっぱ` | 7 | Highlights the beginning of `おっぱい` and `おっぱじまる`; it is a substring. |
| `おも` | 6 | Highlights beginnings of `おもい` and `おもしろい`; it is a substring. |
| `おねえ` | 24 | Always the first part of `おねえさま`; mine the complete term or neither. |
| `おとう` | 6 | Always part of `おとうさん` / `おとうさま`; incomplete as a card target. |
| `おかあ` | 6 | Always part of `おかあさん` / `おかあちゃん`; incomplete as a card target. |

`おお` (335) is also suspicious because its examples mix an exclamation and a
prefix-like `大`; it cannot receive one stable lexical definition. It warrants
context-sensitive handling, not an unconditional blacklist.

## Review queues, not automatic removals

### Noun-plus-grammar phrases (361 rows)

In every shown example, these are a noun followed by a particle, auxiliary, or
suffix. Examples include `ことに`, `たびに`, `今では`, `自分から`,
`久しぶりに`, `何度でも`, `まえに`, and `いくつも`.

Many are useful grammar or collocations, and they should remain in this deck.
Their presence is a prompt to improve how their definitions and target forms
are selected, not a reason to discard them.

### Proper-noun evidence (15 rows)

Every example contains a token Sudachi labels as a proper noun for entries
including `王子様`, `黒魔法`, `ハナから`, `シンと`, `コキ使う`, `竜田揚げ`,
`茶巾ずし`, and `ロゼッタ石`.

This is only a review signal, not an automatic filter. Sudachi sometimes marks
ordinary compounds as proper nouns, while names and game-specific terms can be
useful when they occur in reading material.

### Honorific-prefix compounds (211 rows)

The raw analysis begins with `接頭辞` in every example. Most of these are
legitimate, useful compounds—`お部屋`, `お仕事`, `お弁当`, `お風呂`, and
`ご褒美`—so this category should not be filtered wholesale. It explains why
prefix-based rules alone would be too destructive, but it is valuable
provenance for detecting malformed partials such as `おまつ`.

## Recommended pipeline changes

### 1. Preserve source-token provenance through compound merging

`combine_tokens_jmdict()` currently emits a new token with generic `exp` POS
after a dictionary match. Carry the complete raw component list (at least POS,
surface, and lemma) into the merged token instead. Candidate filtering can then
reliably reject a merge whose components are entirely functional grammar.

This will distinguish lexical, grammatical, and malformed candidates without
removing any of them. It avoids repeating the over-broad kana-noun filter.

### 2. Make surface agreement mandatory for a merge

The merger first tries a concatenation of dictionary lemmas. That permits a
dictionary word to match even when the source *surface* is a different
inflected form: `おまち` (polite stem of `待つ`) can yield `おまつ` (black pine),
and `んし` can yield `ぬし` (master).

Use exact combined source-surface agreement, or an explicit validated
inflection rule, as a confidence signal. Do not treat a lemma-only
concatenation as strong evidence that adjacent tokens form a compound. The
initial use of this signal should be to improve target selection and display,
not to remove candidates.

This is the next highest-confidence implementation target.

**Status (2026-10-26): implemented.** Compound lookup now uses the exact
combined source surface rather than a concatenation of dictionary lemmas. This
prevents unrelated targets such as `おまつ` and `ぬし` while retaining the
source-derived candidate for recall-first mining.

The automated regression suite covers audited grammar, fixed expressions,
kana-only forms, and colloquialisms, alongside the source-surface invariant.

For results from the refreshed export after this fix, see the
[re-audit](anki-output-reaudit-2026-10-26.md).

### 3. Add a conservative boundary/continuation check

Even surface matching is not enough for `おっぱ` inside `おっぱい` or
`おっぱじまる`. Record adjacent raw tokens when a candidate ends immediately
before a token that plausibly continues the same unbroken orthographic word.
This should initially be shown as a confidence/review signal because
colloquial spellings are common in the corpus.

### 4. Keep lexical and grammar-pattern mining together, with explicit labels

Grammar constructions belong in this deck. Retain them alongside lexical
vocabulary, but label their candidate type and eventually use a grammar-aware
definition source. The current dictionary merge mixes those goals without
identifying which type it has produced.

## Regression-test seeds

When implementing the next change, add examples for target-selection and
provenance behavior. Do not treat these as removal tests:

- `ありませんし` → record that the candidate comes from grammar components
- `おまちください` → do not select `おまつ` (black pine) as its lexical target
- `おまちがいない` → do not select `おま` as its lexical target
- `おっぱい` and `おっぱじまる` → record continuation ambiguity
- `かっけえ` and `だりぃ` → preserve as supported colloquial vocabulary

