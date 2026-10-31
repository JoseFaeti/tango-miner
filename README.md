# Tango Miner

Tango Miner extracts Japanese learning candidates from your reading material and
produces CSV files for review or use with Anki. It aims to retain useful words,
expressions, grammar, colloquialisms and names. Grammar cards include selected
〜ことにする decisions, 〜ようだ inference or resemblance, and 〜ば conditions; see the [grammar card details](docs/grammar/card-details.md)
for coverage and limitations. The [priority grammar tracker](docs/grammar-priorities.md)
and [remaining candidates](docs/grammar/remaining-candidates.md) guide future work.

## Getting started

1. Install Python 3.11 or newer (including its `venv` module), then run:

   ```sh
   ./setup.sh
   . .venv/bin/activate
   ```

   The script installs the Python dependencies and the Sudachi full dictionary
   into `.venv`. Set `PYTHON` to choose a Python executable or `VENV_DIR` to
   choose a different virtual environment directory. Obtain a JMdict English
   XML file separately (available from [EDRDG](https://www.edrdg.org/wiki/index.php/JMdict-EDICT_Dictionary_Project))
   and set `[dictionary]` `jmdict_path` in `config.toml` to its uncompressed path.
   For the tested dictionary snapshot, see
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

To receive a phone notification when a run completes or fails, install the ntfy
app on your phone and subscribe to a private, hard-to-guess topic. Set the
`NTFY_URL` environment variable to the topic URL before starting the pipeline.
On Windows, save it for your user account once in PowerShell:

```powershell
[Environment]::SetEnvironmentVariable("NTFY_URL", "https://ntfy.sh/your-private-topic", "User")
```

The full-corpus PowerShell script reads that user setting even if its shell was
already open. For direct Python runs, open a new terminal after saving it, or set
`$env:NTFY_URL` in the current terminal. Use your own server URL if applicable.
Notification delivery is best effort; a
network error is reported without changing the pipeline result. The topic URL is
not saved in the run archive. Without `NTFY_URL`, notifications are disabled.

For configuration, output, diagnostics and development tools, see the
[usage reference](docs/usage-reference.md). For ongoing work and issue tracking,
see [next steps](docs/next-steps.md) and the
[quality analysis](docs/false-positive-analysis.md).
