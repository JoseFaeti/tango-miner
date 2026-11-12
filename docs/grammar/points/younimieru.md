# 〜ように見える

Status: [~]

## Meaning

Looks as if; appears to be.

## Verified coverage

Matches adjacent predicate or noun + の followed by ように見える, including
past, polite, negative and て forms. Analyzer tests cover verbs, adjectives,
adjectival nouns and noun comparisons, plus separated and incomplete forms.
The reserved span begins at よう, leaving the preceding predicate available;
this point owns the overlap with 〜ように. Noun comparisons such as 医者のように
見える are included because they express visual resemblance.

## Remaining limitations

Other inflections, spellings and tokenizations are not covered. Morphology
alone cannot resolve all appearance and comparison readings; precision and
recall have not been independently evaluated.
