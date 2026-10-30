# Engineering foundation

[Contents](../pipeline-improvement-roadmap.md)


1. Establish a small `PipelineContext` containing the resolved config, logger,
   run directory, metrics collector, and dependency handles.  Pass it to steps
   instead of relying on module globals and ad hoc constructor constants.
2. Give each step a declared input/output type and a stable stage name.  A
   `StageResult`/artifact envelope can carry data, provenance, metrics, and
   diagnostics without repeatedly rebuilding bare `Artifact` objects.
3. Define failure policy per stage: a bad input file may be skippable, an absent
   JMdict should fail before processing, and an Anki export failure should leave
   the CSV and its manifest intact while returning a non-zero exit code.
4. Add an end-of-run summary: files accepted/skipped, sentences extracted and
   rejected, cache statistics, raw/kept/dropped candidates, definitions found,
   example coverage, output paths, and Anki plan/result.
5. Pin/package dependencies and add a reproducible development setup.  A fresh
   checkout should state Python version, Sudachi dictionary requirements,
   JMdict location, and test command.

