# Stage and precedence

[Grammar card index](card-details.md)

The `〜ておく` card matches a continuative verb or auxiliary followed by
split て/で + non-independent おく, or a Sudachi とく/どく auxiliary.
Supported complete forms include present, past, negative, polite, て-form,
conditional and volitional forms, including たら as a complete token. The
conditional card can also match the same たら. Ambiguous analyses such as
読んどいた, negative antecedents such as 忘れないでおいて and the 置く
spelling remain lexical. The meaning can be preparation or leaving a resulting
state; tokenization cannot establish which.
A raw replay of the first 20,000 archived sentences found 123 matches, before
expression precedence and example eligibility; this does not measure precision
or recall. In the archived ほっとく example, the current dictionary merger
leaves ほっ and とく separate. Reservation keeps the ほる component and removes
the とく lexical occurrence; the complete expression is not emitted as a
lexical candidate in this example. Independent おく and 置く remain eligible.
When a longer JMdict expression starts at おく and extends beyond the
construction, as in おけばいい or おけばよかった, it takes precedence over
this grammar match. The attached verb remains eligible; in 従っておく, the
merger leaves 従う as a verb rather than selecting the adverb 従って. These
changes are validated on selected examples and have not been compared in a
full corpus. The interpretation and wider frequency effects need further evaluation.

The obligation card `〜なければならない` recognizes selected complete verbal
negative conditional forms followed by `ならない` or `いけない`, including
plain and polite present and past negative endings and `なきゃ`, `なくちゃ`, and `ないと`. It reserves
the suffix while retaining the attached verb; dictionary expression precedence
can suppress the card. Entirely merged verb-and-suffix tokens, conditional past endings and
elliptical obligations remain unsupported. Surface and token boundaries do not
resolve every contextual ambiguity, and precision and recall have not been
independently evaluated.


The `〜てみる` card recognizes an adjacent continuative verb + connective て/で + non-independent みる. It includes the supported present form and selected past, negative and polite forms. Standalone 見る and noun + てみる do not match. These token and POS cues do not prove that the meaning is “try doing”; a literal see reading can remain ambiguous, and unsupported forms stay lexical. The rule requires an eligible example and has not been independently evaluated for precision or recall.

Grammar card meanings use short dictionary-style glosses. Existing Anki grammar
notes retain their current fields under the protection policy described below;
new cards and CSV output use the current glosses.

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

