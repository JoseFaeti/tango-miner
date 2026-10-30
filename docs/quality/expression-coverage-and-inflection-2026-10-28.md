# Expression coverage and inflection (2026-10-28)

[Quality analysis index](../false-positive-analysis.md)


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

