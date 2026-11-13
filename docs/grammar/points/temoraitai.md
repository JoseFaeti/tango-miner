# 〜てもらいたい

Status: [~]

## Meaning

Want someone to do.

## Implementation

The [rule](../../../src/grammar_rules/rules/temoraitai/index.py) recognizes
an adjacent continuative verb and て/で followed by the non-independent
もらい and the たい auxiliary. Its span retains the preceding verb and covers
the connective through the supported たい inflection, including past, negative,
conditional and です forms. The existing 〜たい rule may also match the same
ending; overlapping grammar cards are retained under the shared precedence policy.
The stable identity is `temoraitai`, expression `〜てもらいたい`.

[Focused tests](../../../src/grammar_rules/rules/temoraitai/test_index.py)
cover source tokenization, controls, reservation and card identity. Merged
tokenizations, other spellings and variants such as いただきたい remain
unassessed. Morphology alone cannot establish the intended benefactive reading;
corpus precision and recall have not been evaluated.
