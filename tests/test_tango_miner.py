import unittest
import tempfile
import json
from pathlib import Path
from unittest.mock import patch

from tango_miner import MIN_FREQUENCY_DEFAULT, process_script, resolve_directory_output_path
from src.steps.AddWordsToAnkiStep import AddWordsToAnkiStep
from src.steps.AddDefinitionsStep import AddDefinitionsStep
from src.steps.WriteOutputStep import WriteOutputStep
from src.steps.TokenizeStep import RawTokenizeStep
from src.steps.GatherInputFilesStep import GatherInputFilesStep
from src.steps.FilterFrequencyStep import FilterFrequencyStep
from src.steps.ScoreWordStep import ScoreWordStep
from src.steps.AttachSentencesStep import AttachSentencesStep
from src.steps.MergeTokensStep import MergeTokensStep
from src.steps.OccurrenceDiagnosticsStep import OccurrenceDiagnosticsStep


class TangoMinerTests(unittest.TestCase):
    def test_occurrence_diagnostics_are_opt_in_and_configure_both_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / 'config.toml'
            config.write_text('', encoding='utf-8')
            for enabled in (False, True):
                with self.subTest(enabled=enabled), patch('tango_miner.Pipeline') as pipeline:
                    pipeline.return_value.duration = 0
                    args = ['--config', str(config), '--input', tmp]
                    if enabled:
                        args.append('--occurrence-diagnostics')
                    process_script(args)
                    steps = pipeline.call_args.kwargs['steps']
                    self.assertEqual(sum(isinstance(s, OccurrenceDiagnosticsStep) for s in steps), int(enabled))
                    writer = next(s for s in steps if isinstance(s, WriteOutputStep))
                    self.assertEqual(writer.diagnostics_path is not None, enabled)

    def setUp(self):
        dependency_patch = patch("tango_miner.collect_dependencies", return_value={})
        dependency_patch.start()
        self.addCleanup(dependency_patch.stop)

    def test_failed_pipeline_writes_error_and_stage_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.toml"
            config.write_text("", encoding="utf-8")
            with patch("tango_miner.Pipeline") as factory:
                pipeline = factory.return_value
                pipeline.run.side_effect = RuntimeError("example failure")
                pipeline.stage_records = [{"name": "ReadFilesStep", "status": "failed"}]
                pipeline.duration = 0.1
                with self.assertRaisesRegex(RuntimeError, "example failure"):
                    process_script(["--config", str(config), "--input", tmp])
            summary_path, = (Path(tmp) / "runs").glob("*/summary.json")
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "failed")
            self.assertIn("example failure", summary["error"])
            self.assertEqual(summary["stages"][0]["name"], "ReadFilesStep")
            self.assertEqual(summary["anki"]["status"], "disabled")

    def test_pipeline_receives_discovery_and_filter_policies(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.toml"
            config.write_text('[input]\nextensions = [".txt"]\n[tokenizer]\nconfig_path = "custom.json"\ndictionary = "small"\n[dictionary]\njmdict_path = "other.xml"\n[merge]\nmax_lookahead = 4\n[filter]\nkeep_percent = 75\nmin_frequency_per_source = 2.0\n[scoring]\nfrequency_weight = 0.4\n[sentences]\nmax_sentences = 2\n', encoding="utf-8")
            with patch("tango_miner.Pipeline") as pipeline:
                pipeline.return_value.duration = 0
                process_script(["--config", str(config), "--input", tmp])
            steps = pipeline.call_args.kwargs["steps"]
            gather = next(step for step in steps if isinstance(step, GatherInputFilesStep))
            filter_step = next(step for step in steps if isinstance(step, FilterFrequencyStep))
            self.assertEqual(gather.extensions, {".txt"})
            tokenizer = next(step for step in steps if isinstance(step, RawTokenizeStep))
            self.assertEqual((tokenizer.config.config_path, tokenizer.config.dictionary), ("custom.json", "small"))
            merge = next(step for step in steps if isinstance(step, MergeTokensStep))
            self.assertEqual(merge.max_lookahead, 4)
            definitions = next(step for step in steps if isinstance(step, AddDefinitionsStep))
            self.assertEqual(merge.dictionary_path, "other.xml")
            self.assertEqual(definitions.dictionary_path, "other.xml")
            self.assertEqual(filter_step.keep_percent, 75)
            self.assertEqual(filter_step.min_frequency_per_source, 2.0)
            scoring = next(step for step in steps if isinstance(step, ScoreWordStep))
            self.assertEqual(scoring.config.frequency_weight, 0.4)
            sentences = next(step for step in steps if isinstance(step, AttachSentencesStep))
            self.assertEqual(sentences.config.max_sentences, 2)

    def test_retokenize_flag_configures_pipeline_step(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.toml"
            config.write_text("", encoding="utf-8")
            for flag in (False, True):
                with self.subTest(flag=flag), patch("tango_miner.Pipeline") as pipeline:
                    pipeline.return_value.duration = 0
                    args = ["--config", str(config), "--input", tmp]
                    if flag:
                        args.append("--retokenize")
                    process_script(args)
                    steps = pipeline.call_args.kwargs["steps"]
                    token_step = next(step for step in steps if isinstance(step, RawTokenizeStep))
                    self.assertEqual(token_step.force_retokenize, flag)
                    pipeline.return_value.run.assert_called_once()

    def test_config_controls_anki_step_after_csv(self):
        for enabled in (False, True):
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory() as tmp:
                config = Path(tmp) / "config.toml"
                config.write_text(f'[anki]\nenabled = {str(enabled).lower()}\ndeck = "Test Deck"\n', encoding="utf-8")
                with patch("tango_miner.Pipeline") as pipeline:
                    pipeline.return_value.duration = 0
                    process_script(["--config", str(config), "--input", tmp])
                steps = pipeline.call_args.kwargs["steps"]
                self.assertEqual(sum(isinstance(step, AddWordsToAnkiStep) for step in steps), int(enabled))
                self.assertFalse(any(step is None for step in steps))
                if enabled:
                    self.assertIsInstance(steps[-2], WriteOutputStep)
                    self.assertIsInstance(steps[-1], AddWordsToAnkiStep)
                    self.assertEqual(steps[-1].config.deck, "Test Deck")

    def test_default_minimum_frequency_is_positive(self):
        self.assertGreater(MIN_FREQUENCY_DEFAULT, 0)

    def test_directory_output_defaults_to_csv_inside_input_directory(self):
        input_path = Path("corpus")

        self.assertEqual(
            resolve_directory_output_path(input_path, None),
            Path("corpus") / "corpus.csv",
        )

    def test_directory_output_existing_directory_gets_default_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "out"
            output_dir.mkdir()

            self.assertEqual(
                resolve_directory_output_path(Path("corpus"), str(output_dir)),
                output_dir / "corpus.csv",
            )

    def test_directory_output_file_path_creates_parent_and_returns_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_file = Path(tmp) / "missing" / "out.csv"

            resolved = resolve_directory_output_path(Path("corpus"), str(output_file))

            self.assertEqual(resolved, output_file)
            self.assertTrue(output_file.parent.exists())

    def test_directory_output_new_directory_gets_default_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = Path(tmp) / "new-output"

            resolved = resolve_directory_output_path(Path("corpus"), str(output_dir))

            self.assertEqual(resolved, output_dir / "corpus.csv")
            self.assertTrue(output_dir.exists())


if __name__ == "__main__":
    unittest.main()
