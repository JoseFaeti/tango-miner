# Pipeline performance measurement plan

## Goal and archive policy

Make it easier to find slow pipeline steps and assess changes as the grammar
rule set grows. Each run should have a compact, dedicated performance file,
but it must contain only measurements or context that cannot already be
recovered from other run artifacts. Do not duplicate existing fields merely
for convenience. Establish the exact schema after auditing the current archive
and the cost of collecting each measurement.

The existing `steps/*.json` records already include step duration, status,
input and output counts, available cache statistics, and grammar occurrence
counts. `config.json`, `input-manifest.json`, `notes.txt`, the Git patch when
present, and run dependency records provide other comparison context. Check
what each archive actually contains before adding any field; missing data in an
older or incomplete run must be reported as unavailable, not guessed.

## Measurements

- Keep existing per-step timing and cache records in their current canonical
  location. Do not copy them into the new performance file. Define timing
  semantics clearly, including that current step duration excludes recorded
  user-wait time.
- Add only new measurements needed to explain cost and not recoverable from
  existing artifacts. Pilot aggregate grammar timings for alignment, rule
  matching, expression precedence, aggregation, and cache key/read/write work,
  as feasible. Define whether phases overlap and how cache hits are represented.
- Consider CPU time, peak memory, and work counts only after checking what is
  already recorded, their collection cost, and platform support. Record
  unavailable measurements explicitly rather than treating them as zero.
- Make detailed per-rule timing opt-in and aggregate it per rule; avoid
  per-sentence logs. Quantify instrumentation overhead against an uninstrumented
  run before deciding which measurements are enabled by default.
- Record run-specific measurement settings or limitations only if they cannot
  be recovered from configuration or another archive artifact. Use a versioned
  schema, and preserve partial or failed-run status without implying completion.

## Aggregation and comparisons

Extend an existing run-inspection or comparison script where suitable, or add a
reusable script under `scripts/`. It should combine the performance file with
canonical step records, configuration, input manifest, revision and patch, and
dependency records on demand. Provide a compact single-run report and a
comparison of selected runs; identify missing artifacts and mismatches rather
than copying their contents into the performance file. Distinguish cache hits,
misses and forced refreshes, and avoid treating a warm run as comparable to a
cold run without saying so.

A single run is diagnostic, not a reliable speedup estimate. Support repeated
controlled measurements and summarize their variation; compare equivalent
inputs, configuration, code and dependency identities as appropriate. Describe
uncontrolled machine load and unavailable environment information as limits.

## Grammar pilot and implementation sequence

1. Audit existing archive fields and inspection scripts; specify the minimal
   performance schema and collection overhead budget.
2. Add the dedicated file and focused grammar phase measurements, including
   opt-in per-rule timings. Ensure failures and cache hits produce interpretable
   records without changing mining behavior.
3. Build the aggregation view and verify it handles older, incomplete and
   failed archives, missing fields and mixed schema versions.
4. Benchmark on a bounded representative fixture or existing archive where
   possible. Compare instrumented and uninstrumented output and timing. Any
   full pipeline run requires a corpus agreed with the user and Anki disabled
   or output-only, following the project run policy.
5. Use the results to prioritize grammar improvements such as shared token
   indexes, finer-grained caching and expression-precedence reuse. Extend
   detailed measurements to other steps only where they answer a concrete
   performance question.

Validation must check that instrumentation leaves candidates, spans, examples,
ordering and cache behavior unchanged, and that reports correctly distinguish
missing measurements from zero. Run the pipeline test suite for implementation
changes. Do not trade recall for faster measurements or matching.
