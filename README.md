# Tango Miner

Tango Miner extracts Japanese learning candidates from your reading material and
produces CSV files for review or use with Anki. It aims to retain useful words,
expressions, grammar, colloquialisms and names. Grammar cards include selected
〜ことにする decisions, 〜ようだ inference or resemblance, and 〜ば conditions; see the [grammar card details](docs/grammar/card-details.md)
for coverage and limitations. The [priority grammar tracker](docs/grammar-priorities.md)
and [remaining candidates](docs/grammar/remaining-candidates.md) guide future work.

## Getting started

1. Install Python, SudachiPy 0.7.0 or newer, the Sudachi full dictionary and `appdirs`, and
   obtain a JMdict XML file. Configure its path in `config.toml`. For the
   tested dictionary snapshot and environment setup, see
   [development setup](docs/usage/development.md#jmdict-test-baseline).
2. Check `config.toml` and `sudachi.config.json`. The current baseline uses the
   full Sudachi system dictionary without a user dictionary. Set `[anki]`
   `enabled = false` for a CSV-only run, or `output_only = true` for a preview.
3. From the repository root, run:

   ```sh
   python tango_miner.py --input path/to/text-directory --output path/to/output.csv
   ```

Results are saved in a timestamped `runs` directory alongside the output path.
Read the archive’s `report.md` for a concise run overview; detailed JSON is available for investigation.
Use `python tango_miner.py --help` to see the available options.

For configuration, output, diagnostics and development tools, see the
[usage reference](docs/usage-reference.md). For ongoing work and issue tracking,
see [next steps](docs/next-steps.md) and the
[quality analysis](docs/false-positive-analysis.md).
