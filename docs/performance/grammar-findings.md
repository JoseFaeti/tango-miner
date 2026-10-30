# Grammar performance findings

[Contents](../pipeline-performance-plan.md)

## Grammar findings and first optimization (2026-10-30)

A measured grammar cache miss took 160.2 seconds: expression precedence took
69.9 seconds, rule matching 52.5 seconds, cache-key preparation 30.3 seconds,
alignment 4.4 seconds and aggregation 1.0 second. These are one run's elapsed
measurements, not a controlled estimate of any speedup. Precedence includes
JMdict access, merging, and alignment; its subphases overlap the parent timing.
A separate warm-cache run spent 35.5 seconds preparing the grammar key, so the
key itself is a substantial cost even when matching is skipped.

The first change resolves and retrieves the JMdict object once per grammar
process invocation, lazily when the first proposal needs precedence, instead
of resolving the same path for every matching sentence. It retains the existing
merger, token stream, overlap rules and dictionary selection. A bounded local
check of 10,000 warm dictionary accesses took 1.34 seconds; this establishes
that repeated path resolution has a cost, but does not establish corpus-level
savings. The new `expression_dictionary`, `expression_merge`, and
`expression_alignment` entries split the existing precedence timing and will
show which part dominates in future runs. They are nested within
`expression_precedence` and must not be added to it.

Next, use those subphase timings and a representative, agreed corpus to assess
whether reusing merge evidence or caching per source is justified. Rule scans
and corpus-wide key preparation remain separate opportunities. Compare source
spans, occurrences, examples and lexical reservation as well as timing; an
optimization must preserve expression precedence and useful grammar recall.

## Follow-up measurement

A later cache-miss grammar stage took 135.5 seconds, including 50.7 seconds
of rule matching, 44.1 seconds of expression precedence, and 33.1 seconds
of cache-key preparation. Within precedence, merging took 32.1 seconds,
dictionary access 8.9 seconds, and merged-token alignment 1.6 seconds. These
subphases are included in the precedence total. This single run is not a
controlled estimate of the dictionary lookup change, and its pipeline was still
running when the grammar stage was inspected.
