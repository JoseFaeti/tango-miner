# Unresolved definitions and Anki preservation

[Quality analysis index](../false-positive-analysis.md)


A missing definition is review status rather than evidence that a candidate is
invalid. Such candidates remain in the existing `.dropped.csv` review output
and their sentence rows carry `review` status. Existing Anki notes for them are
protected from deletion and are not changed; new unresolved candidates are not
exported. Truly absent filtered candidates remain eligible for the existing
Anki deletion plan.

When a short kana lookup would be rejected as a noun, unanimous unmerged
auxiliary or pre-noun component POS can select a single compatible dictionary
entry. Nouns and other POS,
mixed analyses, merged components, unsupported POS, and multiple compatible entries stay in
review; POS alone cannot safely disambiguate noun homophones such as `そば`.
Source-grounded potential forms can receive a labeled definition from a
matching base verb and reading when the full potential form occurs. Other
inflections and ambiguous definitions
remain for review.

When all retained analyses consist of one adjective token with the same
dictionary form and reading, an incompatible frequency-selected entry can be
replaced by a unique JMdict adjective entry matching that reading. For example,
`ふかい` can select “deep” instead of the noun “discomfort”. Mixed or merged
analyses, incompatible readings, multiple matching entries, and already
compatible entries keep the previous selection. This is not a guarantee that
Sudachi's POS is correct; ambiguous existing selections are not automatically
marked for review. Adverbs remain unchanged: corpus inspection showed
misanalysed word fragments and a noun use of `どかん` tagged as adverbs.

Definition output now respects JMdict sense restrictions for the spelling or
reading being looked up. A restriction on the other dimension is retained
because that dimension is not established by the lookup alone. If the chosen
entry has no applicable gloss, the candidate remains available for review;
selection does not yet search alternative entries using these restrictions.

