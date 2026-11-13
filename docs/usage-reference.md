# Pipeline usage reference

[Documentation index](index.md)

- [Running](#running)
- [Run configuration](#run-configuration)
- [Development](#development)
- [Unresolved definitions](#unresolved-definitions)
- [Expression inventory](#expression-inventory)

Tango Miner mines Japanese learning candidates from material the learner intends
to read. It extracts and normalizes sentences, tokenizes them with Sudachi,
merges dictionary-backed expressions, aggregates and filters candidates, adds
JMdict definitions and example sentences, scores them, and writes CSV files for
review or Anki workflows.

The pipeline also checks a [curated expression inventory](usage/expressions.md)
against raw tokens and retains matches alongside their component analyses.

The project prioritizes **recall**: vocabulary, grammar, fixed expressions,
colloquialisms, names, and uncertain cases can all be useful learning material.
A false positive is a demonstrably misleading target or definition, not simply
an unusual expression. Improve questionable analyses with source and POS
context; do not treat suspended cards as a blacklist or add broad exclusions
without assessing lost learning material.

## Running

See [running](usage/running.md).

## Run configuration

See [run configuration](usage/run-configuration.md).

## Development

See [development](usage/development.md).

## Unresolved definitions

See [unresolved definitions](usage/unresolved-definitions.md).

## Expression inventory

See [expression matching](usage/expressions.md).

