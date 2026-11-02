# Grammar stage and precedence

[Grammar documentation](README.md)

The shared grammar stage runs by default; pass `--no-grammar` to disable it.
It checks registered rules and adds grammar candidates to CSV output and, if
Anki is enabled, to the normal plan. Grammar meanings use short dictionary-style
glosses; new cards and CSV output use the current glosses, and existing grammar
note meanings and metrics can be refreshed under the [identity policy](identity-and-review.md).

An eligible source example must also pass the existing sentence-validity check. No card is emitted
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

Dictionary expressions can also take precedence when they start before a grammar span and end inside it, as with ではなさそう + だ and 命に代えて + も. This preserves lexical components but suppresses the overlapping grammar occurrence; the effect on other expressions remains unmeasured.
