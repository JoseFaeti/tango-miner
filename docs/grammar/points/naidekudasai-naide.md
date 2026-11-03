# 〜ないでください / 〜ないで

Status: [~]

## Meaning

Negative request or without doing.

## Implemented behavior

The naide rule matches verbal ない followed directly by connective で, including
passive and causative auxiliaries supported by 〜ない. It extends through an
adjacent imperative ください, 下さい, くれ or 呉れ analyzed as a
non-independent verb. The attached verb remains lexical. Normal dictionary
expression precedence applies. The separate 〜ない card also matches the
negative auxiliary, so both cards may count the same occurrence.

Focused tests cover Sudachi tokenization, requests, without-doing uses, lexical
ない, spacing, reservation, examples and the overlapping negative card.

## Limitations

Morphology alone cannot distinguish requests, deliberate omission, or other
relations. Other request forms and tokenizations are unverified; unsupported
following forms remain outside the ないで span. Precision and recall have not
been independently measured.
