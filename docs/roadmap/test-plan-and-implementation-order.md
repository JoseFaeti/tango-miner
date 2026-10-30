# Test plan and implementation order

[Contents](../pipeline-improvement-roadmap.md)


Implement in small vertical slices, keeping existing behavior as the default
until a deliberate compatibility change is released.

1. Add `RunConfig`, an editable default TOML, config precedence tests, and a
   `--show-config` smoke test.
2. Introduce `PipelineContext`, structured logging, manifest, stage timing, and
   deterministic input ordering.  Add unit tests that assert counts and paths.
3. Preserve provenance (especially `SegmentedSentence.origin`) and add compact
   debug artifacts for extraction, merges, candidate decisions, and sentence
   ranking.  Test schema and stable IDs rather than only text snapshots.
4. Make output atomic and self-describing; add CSV schema/overwrite tests.
5. Put Anki behind explicit opt-in, implement dry-run/plan mode, then test the
   plan with a fake AnkiConnect server.  Only after this should pruning be
   enabled explicitly.
6. Use the new diagnostics to improve contextual definition selection and
   candidate/example ranking.  Add regression fixtures for every confirmed
   false positive or valuable recall case.

Success means a user can rerun a corpus months later with the same configuration
and explain each resulting card—from source sentence through token/merge/filter
decisions to its definition, score, example, CSV row, and optional Anki change.
