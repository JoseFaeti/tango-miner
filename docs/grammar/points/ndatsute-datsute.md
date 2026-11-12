# 〜んだって / 〜だって

Status: [~]

## Meaning

Reported speech or emphasis.

## Verified coverage

At utterance boundaries, matches `んだって` after an explanatory predicate
and `だって` after a noun or adjectival noun, including 学生なんだって and
雨だって. The predicate remains available as lexical evidence. Analyzer tests
cover these forms, reservation and identity, past 読んだって, sentence-initial
だって, 何だって, spacing and non-final quotation. Existing `〜んだ` and
`〜って` rules can also match overlapping spans; normal dictionary-expression
precedence applies.

The bare copula is ambiguous with “even”, and a final 猫だって can still match
in that sense. The rule does not distinguish reported speech from emphatic
assertion or quotation. Non-final uses, pronoun predicates, other punctuation
and analyzer segmentations remain incomplete; corpus precision and recall have
not been measured.
