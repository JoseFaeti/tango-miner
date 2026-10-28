# Sentence analysis improvement plan

Goal: improve sentence-wide Japanese analysis while retaining useful vocabulary,
grammar, expressions, names, colloquialisms, and uncertain candidates. Some
sentences admit multiple valid interpretations; preserve alternatives and allow
abstention rather than forcing certainty.

1. **Prepare the evaluation sample.** Audit the existing raw-line sample for
   duplicate and related source material across development and holdout. Repair
   the known exact duplicate and document remaining grouping limitations. Include
   source text excluded by the current pipeline and targeted difficult cases.
2. **Define and review labels.** Agree on the scope of learning candidates and
   annotate source spans, targets, types, readings, and meanings where known.
   Allow overlapping candidates, acceptable alternatives, missing dictionary
   entries, and uncertainty. Review independently and adjudicate disagreements.
3. **Complete the evaluation tooling.** Extend the candidate scorer beyond
   exact span and target matches to assess readings, meanings, and other useful
   attributes. Report recall, precision, abstentions, coverage, and errors by
   category; keep non-exhaustive and unreviewed material out of recall scores.
4. **Establish the baseline.** Produce source-aligned predictions from the
   current pipeline and score them on the reviewed development set. Record
   alignment failures, filtered material, and the standalone proper-noun gap.
5. **Test contextual alternatives in shadow mode.** Compare the baseline with
   additional analyzer and contextual-model proposals on the same inputs. Give
   each method access to source context and relevant dictionary evidence, allow
   abstention, and record versions, resource costs, and privacy constraints.
6. **Select improvements using held-out evidence.** Develop on the development
   set, then evaluate on the leakage-audited holdout. Compare at matched
   coverage and inspect both corrected errors and lost useful candidates. Do not
   interpret the small, stratified sample as a corpus-wide accuracy estimate.
7. **Introduce occurrence-based analysis.** Preserve each occurrence's source
   span, components, alternative analyses, and uncertainty separately from the
   learning candidates derived from it. Keep repeated occurrences distinct and
   align normalized text back to original source where possible.
8. **Roll out validated changes incrementally.** Add focused recall and error
   regressions, compare corpus output with Anki disabled, and keep unsupported
   interpretations available for review. Before changing note identities,
   test migration, duplicates, user edits, and pruning with a fake Anki service.

Current status: a raw-line analyzer comparison and an exact span-and-target
candidate scorer exist. The sample is not independently annotated, and its
audit found one exact text shared between partitions. No contextual method has
yet been validated on a leakage-audited holdout.
