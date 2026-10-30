# Command-line interface

[Contents](../pipeline-improvement-roadmap.md)


Retain existing options as aliases where possible and use conventional
kebab-case names for new options (for example, keep `--minFrequency` as a
deprecated alias for `--min-frequency`).  Useful additions are:

| Option | Result |
| --- | --- |
| `--config PATH` | load TOML configuration |
| `--write-default-config PATH` | write documented defaults and exit |
| `--show-config` | print resolved configuration and exit |
| `--min-frequency N` | override frequency threshold |
| `--include EXT` / `--exclude GLOB` | refine input discovery without source edits |
| `--encoding NAME` / `--on-decode-error {fail,skip,replace}` | control file reading and report failures |
| `--tag-from {filename,parent,none}` | make tag derivation explicit rather than hard-coding bracketed filenames |
| `--no-cache`, `--cache-dir PATH`, `--clear-cache` | control token cache behavior |
| `--stop-after STAGE` / `--start-at STAGE` | debug or resume a pipeline boundary safely |
| `--debug-dir PATH`, `--debug-stage STAGE`, `--log-level LEVEL` | control observability without mixing output locations |
| `--dry-run` | compute outputs and plans without writing CSV/Anki mutations |
| `--anki`, `--anki-prune`, `--anki-deck NAME`, `--anki-model NAME`, `--anki-url URL` | opt in to and configure Anki export |
| `--overwrite` | acknowledge replacement of an existing output file |
| `--summary-json PATH` | write a compact automation-friendly run summary |

Validate paths and option combinations before expensive dictionary loading.  A
single `--verbose` should never be interpreted as permission to modify Anki.

