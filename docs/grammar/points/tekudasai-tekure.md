# 〜てください / 〜てくれ

Status: [~]

## Meaning

Requests.

## Implemented behavior

The `te_kudasai` card recognizes a verb or auxiliary in continuative form
followed by connective て/で and the non-independent imperative ください,
下さい or くれ (also 呉れ when analyzed as 呉れる). The reserved span starts
at て/で, retaining the attached verb as a lexical candidate. Sentence-final
particles such as よ remain outside the span. Complete dictionary expressions
retain the normal expression precedence.

Sudachi full-dictionary tests cover both connective forms and distinguish
imperative requests from くださいます, くださる, くれる, くれない and
くれた. They also cover standalone ください, separated tokens, and the
negative request ないでください, which belongs to the next point. Reservation,
source examples, and frequency are covered by focused tests.

## Limitations

Only analyzer-labeled imperatives are recognized. Other request constructions,
including questions such as てくれない and negative requests, are outside
this rule; colloquial or differently tokenized forms remain unverified. The
matcher uses morphology rather than discourse context, and its precision and
recall have not been independently measured.
