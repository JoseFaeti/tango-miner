# ておく and overlapping expressions (2026-10-29)

[Quality analysis index](../false-positive-analysis.md)


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
