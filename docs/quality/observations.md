# Observations

[Quality analysis index](../false-positive-analysis.md)


The export contains 178 suspended candidates.

- 133 candidates (about 75%) are kana-only.
- 26 candidates are a single character.
- Several clear false positives are very frequent: `てく` (1,033), `ないし`
  (535), and `しれる` (323).

Consequently, increasing the minimum-frequency threshold is not an effective
fix. These errors are systematic tokenization or dictionary-selection errors,
not rare noise.

Representative examples:

| Candidate | Sentence use | Bad analysis |
| --- | --- | --- |
| `しよう` | `何をしようが` | Noun “use” rather than the verbal construction |
| `さい` | `さいだん` / `さい月` | “years old” instead of the start of a larger word |
| `てく` | `ついてく` | Auxiliary/dependent morphology emitted as vocab |
| `ないし` | `できないし` | Dictionary conjunction rather than `ない` + `し` |
| `ようやく` | “finally” | Incorrectly defined as “summary” |
| `風花` | Persona character name | Incorrectly defined as “flurry of snow” |

