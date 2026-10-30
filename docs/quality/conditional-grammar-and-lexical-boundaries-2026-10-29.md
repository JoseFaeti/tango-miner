# Conditional grammar and lexical boundaries (2026-10-29)

[Quality analysis index](../false-positive-analysis.md)


The first `〜たら` run introduced 13 misleading `気が変` and `わる`
occurrences where the source read `気が変わったら`, reducing `気` and
`変わる` by the same amount. Reserving `たら` had hidden it from the
existing boundary repair. That repair now uses a reserved ending as context
without changing reserved tokens. Targeted tests cover archived examples and
retain the source components; legitimate `気が変` and `わる` uses remain
covered separately. A full-corpus comparison has not yet verified the fix.

