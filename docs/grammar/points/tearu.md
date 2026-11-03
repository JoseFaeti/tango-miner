# 〜てある

Status: [~]

## Meaning

An action has left something in a resulting state.

## Verified coverage

The `te_aru` rule matches adjacent verb continuative + て/で + non-independent
ある or 有る. It includes the dictionary form, past and て forms, polite
positive and negative forms, and ば conditionals. The grammar span starts at
て/で, retaining the preceding verb as lexical evidence. Focused Sudachi and
reservation tests cover these forms, ordinary ある, and ている/てない controls.

## Limitations

The rule recognizes morphology, not intent or transitivity. For example,
歩いてある場所へ行く can be an unrelated sequence with attributive ある;
this ambiguity can produce a false positive. Plain negative てない is shared
with ている and is deliberately not attributed to this rule. Other forms and
tokenizations remain unverified, as does corpus-level precision and recall.
