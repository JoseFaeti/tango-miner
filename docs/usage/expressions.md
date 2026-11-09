# Expression matching

[Contents](../usage-reference.md)

The pipeline checks raw Sudachi tokens against the curated
[`data/expressions.json`](../../data/expressions.json) inventory before grammar
reservation. The initial entries were selected from the
[OpenMWE idiom corpus](https://github.com/nlp-waseda/OpenMWE), which distinguishes
literal from idiomatic examples. Each entry links to a separate reference for
its meaning; its English gloss was written for this project. The corpus's
redistribution terms are included in [OPENMWE-LICENSE.txt](../../data/OPENMWE-LICENSE.txt).

The list is deliberately extensible and is not an exhaustive idiom dictionary.
The canonical spelling and concatenated analyzer reading for each initial
entry were absent from the tested JMdict snapshot. This does not prove that
all spellings or synonymous expressions are absent from every JMdict edition.
Run `python scripts/inspect_expression_coverage.py` to inspect current inventory
coverage, or pass a UTF-8 file with one candidate per line. The script checks
exact written and composed-reading lookups and prints raw tokens as JSON; it
does not decide whether a dictionary sense is equivalent.
Run summaries record the inventory file's SHA-256 alongside the other
dependency identities.

Entries specify the canonical identity, reading, meaning, noun and verb spelling
variants, verb class, particle and meaning reference. Matching normally uses
aligned, contiguous noun, optional listed particle and verb tokens. Sudachi's verb lemma supports
ordinary conjugation, auxiliary chains, ichidan ら抜き potentials and selected
godan potential and contracted causative forms. When Sudachi combines or
misreads the parts, source-aligned fallbacks check generated inflected surfaces
and reanalyze the verb fragment at complete raw-token boundaries. The highlighted
occurrence includes consecutive inflectional auxiliaries and a following
connective て, で or ば. Matching
does not infer arbitrary changes to word order, inserted modifiers, all analyzer
segmentation errors, or every colloquial contraction. Particle omission and
inflection are not proof of an idiomatic meaning.
The fallback also retains merged continuative stems, which can coincide with
independent nouns such as 筆入れ; both analyses remain available for review.

Matched expressions are added as separate candidates with their raw components,
surface span, counts and source tags. Component words and grammar matches stay
eligible, including overlapping constructions such as 〜ている. This avoids
assigning a potentially literal occurrence exclusively to the expression. An
existing lexical candidate with the same identity retains its existing
occurrences; additional nonoverlapping matches are added without double-counting.
The normal frequency and source thresholds apply. The
inventory's gloss is used only if JMdict does not supply a definition.

Because the matcher cannot reliably distinguish idiomatic and literal uses,
inventory candidates are marked for review and written to `.dropped.csv`, with
`expression_may_be_literal` in optional candidate diagnostics. They are not
automatically added to Anki. Their position uses raw eligible token positions;
other lexical candidates use merged token positions. Source components are
retained for example selection. With occurrence diagnostics enabled,
`expression-occurrences.jsonl` records each pre-filter expression match, its
source span, omitted-particle flag, matching rule and raw components; the existing
`occurrences.jsonl` describes the ordinary merged token stream. A count is a
surface-pattern count, not an estimate of idiomatic precision.
