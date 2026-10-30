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
controlled estimate of the dictionary lookup change. The archived pipeline
subsequently completed; its merge stage reported
231 cached source groups (521,821 token streams) in 46.2 seconds. These are
observations from the same run, not a controlled comparison.

## Bounded best-entry reuse

Expression precedence now memoizes up to 8,192 JMdict best-entry selections
within each grammar invocation. This avoids repeating entry ranking for the
same spelling while leaving merge decisions and reservation unchanged. The
cache is discarded after the invocation and does not affect the persistent
grammar cache. In a synthetic check of 12,000 calls using six repeated
spellings, direct selection took 0.102 seconds and memoized selection 0.0014
seconds, excluding dictionary loading. This does not estimate stage-level
savings or demonstrate representative hit rates. Rule scans, merge work, and
cache-key preparation remain separate optimization targets. Focused tests
compare occurrences, spans, examples and reservation with memoization disabled.

## Cache-key preparation follow-up

On a bounded local sample of 10,000 tokenized sentences made by repeating
four short examples, three measurements of input-key representation building
took 0.204--0.237 seconds before and 0.119--0.155 seconds after replacing
generator-based validation with direct loops. JSON serialization took
0.224--0.280 seconds and SHA-256 took 0.030--0.033 seconds. These sequential
microbenchmarks are diagnostic, not a controlled corpus-level estimate.
The encoding retains all token fields, POS list-versus-tuple identity, and
component evidence. Since the shared stage-cache code is fingerprinted, this
change invalidates existing grammar and merge entries once; later warm runs
can reuse newly written entries. Grammar matching and JSON serialization
remain opportunities for further measurement.

## Rule matching follow-up

A profile of 10,000 sentences made by repeating eight short Sudachi-tokenized
examples identified the obligation rule as the most expensive rule on that
sample. It previously rebuilt and scanned the same prefix and ending
combinations inside its token loop. The rule now precomputes complete forms
and their prefixes once, preserving the same matching and token-boundary
checks. Three sequential comparisons against the prior implementation took
0.267--0.272 seconds before and 0.031--0.036 seconds after for this rule,
with identical spans on the sample. This small repeated sample is not a
representative corpus-wide speedup estimate. Changing the rule invalidates
the grammar cache fingerprint; the merge cache is unaffected by this change.

## Bounded archive rule profile

A 20,000-sentence sample from an older grammar-shadow archive was profiled
with `scripts/profile_grammar_rules.py`; its tokenizer configuration predates
the current baseline. The `te_kuru` rule now checks the motion-verb lemma
and connective before more expensive boundary and predicate checks. Three
paired measurements on this sample took 0.103--0.194 seconds before and
0.041--0.047 seconds after; all 214 match spans were identical. The first
before measurement was notably slower. These measurements do not establish
current corpus-wide savings. The grammar cache fingerprint changes when this
rule changes. On this sample `te_iru`, `te_shimau` and `to_omou` were among
the next slowest; profile current-baseline data before selecting further work.

## Expression merge boundary checks

A bounded profile of 2,901 current-tokenizer input streams showed repeated
checks for reserved tokens in overlapping candidate ranges. The merger now
computes the next reserved boundary once per stream, while keeping candidate
order and merge guards unchanged. A paired comparison used the same 2,901
streams, with a reserved token added to 837 streams: three measurements were
0.358--0.368 seconds before and 0.289--0.310 seconds after, with identical
merged tokens and rejected-merge diagnostics. This is a bounded sample, not
a corpus-wide estimate; the profile includes streams without grammar proposals.
Both grammar and merge caches invalidate because their dependencies include
the merger. String joining and dictionary lookup remain visible costs; measure
those on proposal-bearing current-baseline inputs before changing merge logic.
