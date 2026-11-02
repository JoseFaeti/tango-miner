# Negative grammar and expression boundaries (2026-10-29)

[Quality analysis index](../false-positive-analysis.md)


A run introducing verbal `〜ない` removed nine previously valid expressions,
including `ないといけない` and `なくてもよい`; its Anki plan deleted their
notes. The expression-precedence check now also recognizes a longer dictionary
merge that starts at the grammar span. A targeted replay of one archived example
for each of the nine expressions preserved all nine with the configured
analyzer and dictionary. This is not a full-corpus comparison and does not
establish their final frequency or resolve any existing sense ambiguity. Existing
deleted notes require separate recovery; a new run alone cannot restore user edits.

