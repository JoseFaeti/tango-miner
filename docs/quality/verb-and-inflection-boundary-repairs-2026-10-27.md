# Verb and inflection boundary repairs (2026-10-27)

[Quality analysis index](../false-positive-analysis.md)


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

