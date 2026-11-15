# 〜し〜し

Status: [~]

## Meaning

Multiple reasons or examples.

## Implemented behavior

Recognizes connective し immediately after a Sudachi terminal-form verb,
adjective, or auxiliary, including plain, past, negative, copular and polite
predicates. Each し is counted separately, including a single reason; the
matched span is the particle, leaving the predicate available for vocabulary.
Focused Sudachi tests cover repeated and single reasons and distinguish verb
する and lexical 寿司. The matcher also checks token boundaries and particle POS.

This is a syntactic approximation: it does not determine whether adjacent
clauses express reasons or examples, and analyzer errors can affect recall or
precision. Forms without a terminal-form predicate analysis are not covered.
Corpus-level precision and recall remain unverified.
