# Agent command reference

[Project README](../README.md) | [Development guide](../docs/usage/development.md)

From the repository root, run an exposed script as
`python -m tango_miner.COMMAND`, using the arguments accepted by its underlying
script. For example:

```sh
python -m tango_miner.next_grammar
python -m tango_miner.inspect_run --help
```

Every module accepts `--help` for usage and `--silent` to suppress standard
output and standard error, including error messages. Silent mode preserves
file output and exit status; check the status to detect failures. These shared
options apply to the module interface; the original scripts keep their own
options. Corpus execution and dictionary modification scripts are not exposed
through this interface.

| Command | Purpose |
| --- | --- |
| `next_grammar` | Print the first unchecked grammar queue entry as JSON. |
| `check_grammar_docs` | Check grammar indexes, specifications, identities and links. |
| `replay_grammar` | Replay a registered grammar rule against a historical aligned-token archive. |
| `profile_grammar_rules` | Profile grammar rules on a bounded historical aligned-token sample. |
| `inspect_run` | Inspect a completed run with bounded output. |
| `compare_runs` | Compare archived mining results without rerunning mining or contacting Anki. |
| `query_jsonl` | Search JSONL diagnostics without loading the whole file into memory. |
| `inspect_anki_plan` | Summarize changes in an archived Anki preview. |
| `report_performance` | Show performance measurements alongside run records. |
| `evaluate_review` | Create and score independently labeled review samples. |
| `score_sentence_candidates` | Score independently reviewed sentence candidates. |
| `compare_japanese_analyzers` | Build a bounded raw-source benchmark and compare analyzers offline. |
| `check_jmdict` | Check the configured JMdict against the tested snapshot. |
| `run_tests` | Run the test suite or selected unittest tests. |

See the [development guide](../docs/usage/development.md) for examples and
limitations of individual tools. `runner.py` is an internal helper, not an
exposed command.
