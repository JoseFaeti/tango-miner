# Contextual homophone selection (2026-10-27)

[Quality analysis index](../false-positive-analysis.md)


For candidates with multiple dictionary entries, single-token analyses can
identify a unique entry by reading, POS, and available verb conjugation. The
most frequent supported entry supplies the definition; ties or analyses that
cannot distinguish entries remain for review. Examples are selected only from
occurrences supporting that entry, including the fallback path. Frequency and
score still count all candidate occurrences. Optional candidate diagnostics
report the supporting and other occurrence counts; occurrence diagnostics
retain the individual analyses. A non-independent verb can select a unique
entry with an auxiliary sense, but this is not full sentence-level semantic
analysis. Merged analyses and other unmatched occurrences are not assigned to
the selected definition. Single-entry and evidence-free lookups retain their
existing behavior. The output does not yet create separate cards or present
alternative definitions for other groups.

A scan of locally cached streams found mostly suru-verb analyses for `する`,
mostly ichidan non-independent analyses for `いる`, and mostly adverbs for
`ようやく`, alongside other or merged analyses for each. The scan covered
matching cached streams, not exactly the archived filtered occurrences.

A full-corpus check with Anki disabled used the same input manifest as the
latest archived run. Candidate counts and frequencies were unchanged. The
three target words received compatible definitions, while valid output fell
from 19,032 to 18,037 and review rose from 1,201 to 2,196 (a net 995 more
review candidates). This is a material change to normal output; the review
file preserves those candidates, but unresolved new candidates are not added
to Anki. This aggregate comparison does not establish whether each newly
unresolved analysis is genuinely ambiguous. Review and improve coverage before
considering the broader selection policy settled.


