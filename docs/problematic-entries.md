# Problematic output entries

Evidence snapshot: completed run `2026-10-27_1701_8ef69e9-dirty`.
Frequency is the candidate's total recorded corpus count, **not** the number
of incorrect occurrences or cards. Findings below concern displayed examples;
other occurrences may be valid. Some fixes were made after this run, as noted.
Use frequency to prioritize investigation, not to filter candidates. See the
[false-positive analysis](false-positive-analysis.md) for context.

## Incorrect or context-mismatched definitions

- **48,034 — `する`:** “rub; squander” for displayed uses of “do”.
- **32,609 — `いる`:** “need” for displayed auxiliary uses.
- **314 — `ようやく`:** “summary” for displayed uses meaning “finally”.
- **146 — `により`:** “similarity” for means or accordance.
- **146 — `覆われ`:** noun “cover” for passive uses of `覆う`.
- **116 — `おって`:** noun “pursuer” for auxiliary `おる` uses.
- **56 — `風花`:** snow definition for displayed Persona character-name uses;
  valid non-name senses must remain available.

## Inflection, expression, and mixed-occurrence ambiguity

- **153 — `つれて`:** displayed uses are `連れる`; proportional sense is
  unrelated to those examples.
- **90 — `にかけて`:** two displayed examples use `掛ける`; one swears by
  something. Preserve both analyses.
- **69 — `つかる`:** displayed examples include genuine `浸かる` and one
  `使う` use. Investigate occurrence assignment.
- **46 — `さい`:** displayed examples include fragments in `さい月`,
  `くにゃさい`, and `ごめんなさい`; genuine age uses can remain useful.
- **37 — `ないし`:** displayed examples use `ない` + `し`, not the selected
  conjunction. A single-token case remains unresolved; genuine `ないし` is valid.

## Recheck against current code

- **215 — `しれる`:** archived examples include `やもしれん` and
  `かもしれません`, alongside a genuine `知れる` use. The subsequent
  `かもしれない` correction covers some constructions, not `やもしれん`;
  verify which occurrences remain mixed.
- **110 — `疲れた`:** archived examples include past `疲れる`; a subsequent
  narrow past-verb merge guard addresses some contexts, not all.
- **26 — `わる`:** archived examples include fragments of `終わらせる`;
  the later `気が変わった` repair addresses a different failure. Recheck both.
- **16 — `ことし`:** archived examples use `こと` + `する`; subsequent
  boundary corrections cover certain inflections. Preserve genuine “this year”.

Counts and examples are historical; rerun or inspect occurrence diagnostics
before claiming that any issue persists in current output.
