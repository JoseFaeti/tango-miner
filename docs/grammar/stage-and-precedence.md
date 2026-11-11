# Grammar stage and precedence

[Grammar documentation](README.md)

Grammar is a standard part of the pipeline and runs by default; pass
`--no-grammar` to disable it. The stage checks registered rules and adds grammar candidates to CSV output and, if
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
The `〜直す` rule is an exception: its whole compound remains available to
lexical processing so that the vocabulary reading is retained alongside grammar.
Standalone `ある / いる` matches likewise remain available to lexical processing.
The separate [curated expression inventory](../usage/expressions.md) is matched
on raw tokens before reservation and added alongside grammar and component-word
candidates. An inventory match does not suppress an overlapping grammar rule;
its ambiguous expression candidate is retained for review.
The attached verb outside the highlighted span remains eligible. Other occurrences
of the same word remain lexical candidates. A rule reserves tokens only if it
has an eligible example for a card, and only at complete token boundaries.
The merger retains reserved neighboring tokens as context for inflection guards.
These decisions change lexical frequencies, examples and potentially output status; overlapping grammar rules may still match
the same tokens. This is a narrow evidence rule, not a guarantee
that every occurrence has a single unambiguous aspectual meaning.

A replay of the first 20,000 sentences of an earlier diagnostic archive found
214 `〜てくる` matches and 206 reserved raw `くる` tokens, alongside
1,627 `〜ている` and 1,043 `〜んだ` matches. These are ordered,
source-biased raw-token counts, not post-merge frequency changes or a precision
and recall evaluation. Treating a match exclusively as grammar can remove a
useful lexical analysis when the rule is wrong; unmatched uses remain eligible.

Dictionary expressions can also take precedence when they start before a grammar span and end inside it, as with ではなさそう + だ and 命に代えて + も. This preserves lexical components but suppresses the overlapping grammar occurrence; the effect on other expressions remains unmeasured.

An exact kana-only dictionary verb marked as an abbreviation can also take
precedence when Sudachi splits it into a verb and a supported contracted
auxiliary. For example, ほっとく remains a lexical candidate with component
evidence, while 買っとく remains eligible for 〜ておく. This narrow rule
does not establish the interpretation of every homograph or cover inflected
dictionary abbreviations. Corpus frequency and precision effects are unmeasured.

In the archived `はなれちゃっても` example, focused integration tests
retain both 〜てしまう and 〜ても, with their shared て token reserved once.
The archived `乗ってあげてもいい` example likewise produces both the
benefactive and permission proposals under current precedence. These examples
do not explain earlier aggregate decreases. One historical comparison changed
both inputs and configuration; another used the same inputs and configuration
but did not isolate individual competing spans. Neither proves that extending
〜てしまって caused those decreases.

Use `python scripts/inspect_grammar_overlap.py SENTENCES.txt` to inspect up to
20 UTF-8 source sentences, one per line. The JSON report shows raw grammar
proposals, accepted spans, dictionary merges and tokens before and after
reservation. Reservation depends on the supplied sentences providing eligible
card examples; the report does not measure corpus precision or recall.
