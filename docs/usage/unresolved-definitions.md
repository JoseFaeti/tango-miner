# Unresolved definitions

[Contents](../usage-reference.md)


Candidates without a dictionary definition are written to the existing
`.dropped.csv` review file, with their examples also marked `review` in the
sentence CSV. They are not classified as invalid solely because a definition
is missing. Existing Anki notes for those candidates are protected from
deletion and are not updated; new unresolved candidates are not added to Anki.
Candidates absent from the filtered results can still be included in the Anki
deletion plan. Short kana candidates can use unanimous retained token POS to
select a unique compatible JMdict entry for auxiliaries and pre-noun forms.
When a candidate has multiple dictionary entries, retained single-token POS,
reading and verb-conjugation evidence can identify a compatible entry. The
most frequent uniquely identified entry supplies the definition, and examples
are restricted to matching occurrences. Ties and analyses that cannot identify
an entry remain for review. Counts still include all occurrences; optional
candidate diagnostics report how many support the selected definition. Cases
with sentence-initial `しかし、` use that source context as evidence for the
conjunction; other occurrences do not supply its examples. The other uses are
still included in the total frequency and do not get a separate review card.
Occurrence diagnostics capture raw components before this contextual cue is
added during aggregation. Cases
without component evidence retain the existing frequency-based lookup. Short
nouns and other unresolved analyses remain for review. Source-grounded potential forms may use a
labeled definition derived from an unambiguous matching base verb when the
full potential form occurs in the source.
