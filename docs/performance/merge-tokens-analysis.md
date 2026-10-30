# Merge token performance assessment and plan

[Contents](../pipeline-performance-plan.md) · [Performance implementation plan](implementation-plan.md)

This is a code review of proposed optimizations, not a profile or a measured
speedup. The relative costs and frequency of these paths are unknown. Preserve
source-grounded candidates, component provenance, grammar reservations and
rejection diagnostics; do not trade recall for throughput.

## Assessment

1. **Diagnostics I/O.** `process` initializes a temporary file when diagnostics
   are enabled and reopens it only for sentences with rejection events, not
   every sentence. With diagnostics disabled there is no file I/O here, and the
   merge stage cache is disabled when diagnostics are enabled. Keeping one
   buffered stream open is plausible, but its impact depends on event density
   and has not been measured. Preserve event order, UTF-8 JSONL, empty output,
   closure on exceptions and replacement of the destination only after success.
2. **Repair passes.** `repair_run` applies five linear passes in sequence to
   each lexical run. They remain O(N) overall, with five traversals and list
   allocations; significance depends on sentence lengths and other costs. A
   naive single pass is not necessarily equivalent: later repairs see earlier
   transformed tokens and their surrounding context. Reserved boundaries,
   precedence, overlaps and component evidence must be preserved. Fuse only if
   profiling justifies the complexity and equivalence can be demonstrated.
3. **Tokenizer calls.** Imperative repair tokenizes a candidate lemma and
   sometimes its base after restrictive guards; the 気が変 repair tokenizes
   only a specific matched construction. Neither frequency nor FFI cost is
   established. A cache keyed only by text and mode can become stale when the
   active tokenizer or dictionary changes. Scope any bounded cache to tokenizer
   identity/lifetime and mode, account for memory, and cache immutable extracted
   values rather than assuming returned Sudachi objects are safe to retain.
4. **POS normalization.** Several matching guards repeatedly lowercase tags;
   their cost is unmeasured. JMdict loads raw POS text into shared entry dicts,
   used elsewhere for selection and output. Changing that representation at
   load time could alter other consumers. Consider cached derived lowercase
   tags or classifications while preserving raw data, selection semantics,
   dictionary identity and cache invalidation; measure initialization and memory.
5. **Substring matching.** The existing test deliberately uses substring
   matching, while other guards use prefixes and exact comparisons. Replacing
   it with set membership changes semantics for longer tags containing a target.
   Inspect the actual dictionary tag inventory and compare every classification,
   including synthetic longer and mixed-case tags. Keep substring behavior
   unless equivalence is proven; changing eligibility is a separate recall and
   quality decision, not a performance-only refactor.
6. **Field access.** Real `JMDict` parses entries and senses as dictionaries,
   but `_get_field` also supports attribute objects and string POS in callers.
   Test doubles or other consumers may rely on this flexibility. Type-check
   overhead is unmeasured; establish the supported contract and profile before
   narrowing it. Direct dictionary access is not automatically equivalent.

## Implementation plan

Status: `[ ]` not started. Execute only the opportunities supported by evidence;
revisit the ranking after each measured change.

- [ ] Establish a bounded, reusable merge-stage benchmark and profile using
  representative token streams, dictionary and tokenizer identities. Inspect
  existing archives with `scripts/report_performance.py` first; record stage
  time, cache state, diagnostics enabled/disabled, event density, token counts,
  dictionary hits and tokenizer calls as needed. Separate dictionary loading,
  warmup and initialization from steady-state measurements. Repeat equivalent
  runs, report variation and instrumentation overhead, and compare outputs.
  Bounded stage fixtures do not require a full pipeline run. Agree a curated
  corpus before any full run, verify Anki is disabled or output-only, save the
  verbose log and wait for completion before inspection.
- [ ] If diagnostics I/O is material, open the temporary stream once and close
  it before replacement. Test no events, multiple sentences and ordered events,
  disabled diagnostics, exceptions and preservation of the prior destination;
  compare the actual JSONL and merged output with the baseline.
- [ ] If tokenizer work is material, measure hit rate and a bounded,
  tokenizer-aware cache of immutable results. Test a changed tokenizer identity,
  mode, negative results and existing imperative and 気が変 cases, including
  component evidence and reserved grammar boundaries.
- [ ] If dictionary validation is material, evaluate derived POS caching and
  representation costs without changing tag semantics or raw entries. Test
  mixed case, substring and prefix behavior, multiple senses, supported mock
  representations and useful kana-only candidates alongside rejection cases.
  Examine the dictionary inventory before considering exact membership.
- [ ] If repair passes are material, prototype fusion only with the sequential
  passes as an equivalence oracle. Cover interacting and adjacent repairs,
  overlapping matches, runs separated by reserved tokens, output ordering,
  provenance and diagnostics, plus useful positive and negative examples.
- [ ] For each accepted optimization compare complete token streams, component
  evidence and rejection events against baseline, then check downstream
  candidates, spans, examples, ordering and cache invalidation as applicable.
  Run `python -m unittest discover -s tests`, investigate failures and report
  unavailable integration dependencies. Measure repeated before/after timing
  and memory on the same inputs and configuration; state limitations, analyze
  remaining bottlenecks and identify the most useful next step.
