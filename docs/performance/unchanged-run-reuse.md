# Unchanged-run reuse proposal

[Contents](../pipeline-performance-plan.md) · [Implementation plan](implementation-plan.md)

## Goal

When inputs, relevant configuration, dependencies and mining behavior have not
changed, avoid executing deterministic intermediate stages. A near-one-second
unchanged computation path is an aspiration to measure, not a promised total
command time. Existing tokenizer, grammar and merge caches do not yet provide
this: grammar cache-key preparation alone took about 32 seconds in a completed
run, and later stages still execute.

## Proposed design

- Discover the current inputs and build a trustworthy run identity covering
  input contents and relevant paths, tags, extraction and normalization;
  relevant configuration; tokenizer and dictionary contents; project code and
  shared helpers; runtime and dependency versions; and snapshot schema. Include
  all other dependencies actually used by the cached computation. A Git commit
  alone is insufficient when working-tree or untracked code can affect it.
- On an exact validated identity match, restore a complete, isolated snapshot
  of the deterministic result at a boundary chosen after inspecting downstream
  dependencies. Include candidates, definitions, frequencies, scores, examples,
  ordering, review status, source spans, component evidence and provenance.
  Skip only the stages represented by that snapshot. If identity or snapshot
  validation fails, fall back to ordinary execution and stage caches.
- Rediscover inputs and create a fresh run archive and requested outputs and
  diagnostics. Define which diagnostics can be faithfully reconstructed from
  the snapshot; recompute or bypass reuse when they cannot. Never reuse an Anki
  plan or apply operation: read the live deck and generate a fresh plan on
  every enabled run, with existing preview and confirmation behavior.
- Use versioned, integrity-checked storage, atomic writes and safe decoding;
  handle corruption, concurrent runs, refresh requests and mutable state. Set a
  size or cleanup policy and account for source text and paths in cached data.
  Partial input changes should use independently invalidatable stage caches
  rather than requiring the unchanged-run fast path to succeed.

## Cost and correctness

Input discovery, content hashing, dictionary verification, snapshot loading,
output generation and optional live Anki operations all take time. File size
and timestamps alone do not prove content identity. Any faster identity scheme
must state its trust assumptions and detect replacements or edits that preserve
metadata; otherwise use a conservative fallback. Do not sacrifice invalidation
correctness to claim a one-second run.

Before implementation, measure these costs on a bounded representative
fixture and decide where reuse provides the most benefit. Validate cached and
uncached candidates, definitions, examples, spans, provenance, review status,
ordering and diagnostics. Test input additions, deletions, content and path
changes, settings, code and helper changes, dependency changes, refresh,
corruption and interrupted or concurrent writes. Verify that tests do not
mutate Anki and that enabled runs regenerate plans from live state. Measure
repeated warm runs separately from cold and changed-input runs; report full
command time as well as skipped computation and unavailable measurements.

## Initial identity-cost observation

A local bounded check hashed eight input files totaling 7.8 MB in 0.025
seconds and the two recorded dictionary files totaling 421.5 MB in 1.055
seconds. This was one observation with unspecified filesystem cache state; it
does not measure all input files, discovery, startup, snapshot loading, output
or Anki. It shows that reliable content hashing can be a meaningful part of
a one-second target. Measure repeated cold and warm cases before choosing an
identity strategy, and preserve content verification when metadata is unchanged.

## Snapshot boundary and bounded payload experiment

After `AttachSentencesStep`, `artifact.data` contains the scored `WordStats`
objects with attached examples. `WriteOutputStep` reads these objects and may
write candidate diagnostics, `CompareOutputStep` reads the newly written CSVs
and prior run, and `AddWordsToAnkiStep` reads the words and queries the live
deck. A candidate snapshot immediately after sentence attachment could skip
mining while allowing these three stages to execute normally. It must preserve
all `WordStats` fields, insertion order, example spans, component counts,
selected-entry evidence and review status. Occurrence diagnostics, rejected
merges, grammar-shadow and debug output depend on earlier stages and need
equivalent replay or a fallback; do not silently omit them.

`scripts/benchmark_snapshot_payload.py` measures JSON round-tripping the
three existing output CSVs only, as a partial payload proxy. In one completed
run, the files totaled 26.2 MB; reading them took 0.044 seconds, encoding and
checksumming the JSON payload took 0.205--0.211 seconds, and verifying and
decoding took 0.150--0.172 seconds across three repetitions. These warm local
measurements exclude writes, identity checks and full `WordStats` state. The
CSV data is not a usable snapshot and these timings cannot predict full
snapshot cost or guarantee a one-second command.

Next, prototype a versioned safe representation for complete candidate
objects and benchmark its size and round trip on representative data. Verify
CSV and candidate-diagnostic equality and independent mutable state before
connecting reuse to the pipeline; include live Anki plan regeneration and
fallback for requested earlier-stage diagnostics.
