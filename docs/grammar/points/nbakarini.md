# 〜んばかりに

Status: [~]

## Meaning

As if about to do something; to such a degree that it seems imminent.

## Implemented

Matches verbal irrealis plus んばかりに, including Sudachi's separate
auxiliary and merged verbal forms. Focused tests cover analyzer forms, controls,
reservation, lexical verb preservation and card identity. The merged form's
match begins inside the verb, so its tokens are not reserved by the current
whole-token reservation mechanism.

## Limitations

Sudachi may classify せん as a noun, which this rule does not match. Unusual
tokenizations and corpus precision and recall remain unassessed. Context can
affect interpretation; んとばかりに is a separate construction.
