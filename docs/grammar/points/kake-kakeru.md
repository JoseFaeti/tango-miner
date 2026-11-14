# 〜かけ / 〜かける

Status: [~]

## Meaning

An action started but left unfinished, or an event on the verge of happening.

## Implementation

The existing [〜かける rule](kakeru.md) also matches attributive 〜かけの,
while [〜かけだ](kakeda.md) covers selected copular forms. The existing
`〜かける` card identity is retained. For 〜かけの, the matched span includes
the verb stem and の; merged nominal stems and a separate nominal suffix after
a continuative verb are supported. The lexical candidate remains available.
Sudachi full-dictionary tests cover 食べかけの and the control 仕掛けの;
split tokens and source boundaries are tested separately.

The merged-noun surface heuristic can also match unrelated words such as
きっかけの; form alone cannot confirm the intended meaning. Other nominal
constructions and analyzer segmentations remain unverified. The related rules
document further ambiguity and unsupported forms.
