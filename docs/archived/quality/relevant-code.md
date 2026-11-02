# Relevant code

[Quality analysis index](../false-positive-analysis.md)


- `src/steps/TokenizeStep.py`: initial token-level filtering.
- `src/steps/MergeTokensStep.py`: dictionary-based compound merging.
- `src/TokenProvenance.py`: immutable raw component evidence.
- `src/WordStats.py`: counts of component sequences per eligible candidate.
- `src/JMDict.py`: entry selection and definition construction.
- `src/steps/BuildWordStatsStep.py`: candidate aggregation.
- `src/steps/FilterFrequencyStep.py`: frequency filtering; this should not be
  treated as the primary false-positive control.

See also [the initial output audit](../archived/anki-output-audit-2026-10-26.md) and the
[18,855-entry re-audit](../archived/anki-output-reaudit-2026-10-26.md).

