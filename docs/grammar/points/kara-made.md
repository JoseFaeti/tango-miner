# から / まで

Status: [~]

## Meaning

Starting and end points.

## Verified behavior

- Separately tokenized case-particle から after a noun or pronoun and
  particle まで after a noun, pronoun, verb, or auxiliary produce one shared
  card. Each marker is counted separately and reserved; its attached word
  remains lexical evidence.
- Focused Sudachi and integration tests cover spatial and temporal endpoints,
  predicate + まで, causal から, てから, deadline までに, degree uses,
  token boundaries, reservation, examples, and card identity.
- The matcher excludes までに and common degree contexts, but cannot reliably
  distinguish every degree, additive, or endpoint reading. It does not cover
  all possible attachments or analyzer tokenizations; longer expressions may
  overlap unless dictionary expression precedence applies. Further source
  review is needed before treating this point as complete.
