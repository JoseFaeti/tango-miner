# Grammatical occurrence corrections

[Quality analysis index](../false-positive-analysis.md)


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

