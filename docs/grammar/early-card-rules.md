# Card rules: explanatory and aspectual

[Grammar card index](card-details.md)

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

