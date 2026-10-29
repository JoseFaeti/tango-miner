import contextlib
import io
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.check_corpus import config_toml, main as check_main
from src.RunConfig import resolve_config
from tests.test_compare_runs import make_run

ROOT = Path(__file__).resolve().parents[1]


class RunInspectionTests(unittest.TestCase):
    def test_inspect_latest_limits_output_and_handles_missing_words(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            make_run(root, '2026-10-27_1200_abc', [('猫', 10), ('犬', 3)])
            result = subprocess.run([sys.executable, '-X', 'utf8', str(ROOT / 'scripts' / 'inspect_run.py'),
                                     '--runs-dir', str(root), '--word', '猫', '--word', '鳥',
                                     '--examples', '0', '--width', '5'], capture_output=True, text=True, encoding='utf8')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('猫 | valid | frequency 10', result.stdout)
            self.assertIn('Absent: 鳥', result.stdout)
            self.assertNotIn('example:', result.stdout)
            self.assertIn('defi…', result.stdout)

    def test_occurrences_summarize_evidence_without_printing_whole_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            run = make_run(root, '2026-10-27_1200_abc', [('猫', 10)])
            row = {'candidate': '猫', 'sentence': '猫はここにいる。',
                   'components': [{'lemma': '猫', 'pos': ['名詞', '普通名詞']}]}
            (run / 'occurrences.jsonl').write_text('\n'.join(json.dumps(row, ensure_ascii=False) for _ in range(5)), encoding='utf8')
            result = subprocess.run([sys.executable, '-X', 'utf8', str(ROOT / 'scripts' / 'inspect_run.py'),
                                     str(run), '--word', '猫', '--occurrences', '--examples', '1'],
                                    capture_output=True, text=True, encoding='utf8')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('猫 occurrences: 5', result.stdout)
            self.assertEqual(result.stdout.count('sample:'), 1)

    def test_inspection_displays_optional_review_reasons(self):
        with tempfile.TemporaryDirectory() as tmp:
            run = make_run(Path(tmp), '2026-10-27_1200_abc', [('猫', 10)])
            row = {'candidate': '猫', 'review_reasons': ['expression_frequency_or_source_coverage']}
            (run / 'candidate-diagnostics.jsonl').write_text(json.dumps(row), encoding='utf8')
            result = subprocess.run([sys.executable, '-X', 'utf8', str(ROOT / 'scripts' / 'inspect_run.py'),
                                     str(run), '--word', '猫'], capture_output=True, text=True, encoding='utf8')
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('review reasons: expression_frequency_or_source_coverage', result.stdout)

    def test_check_corpus_disables_anki_and_redirects_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / 'config.toml'
            config.write_text('config_version = 1\n[input]\npath = "corpus"\n[anki]\nenabled = true\noutput_only = false\n', encoding='utf8')
            output = root / 'output'
            def fake_run(command, **kwargs):
                child_config = resolve_config(command[command.index('--config') + 1])
                self.assertFalse(child_config.anki.enabled)
                self.assertFalse(child_config.anki.output_only)
                self.assertEqual(child_config.input.path, 'corpus')
                self.assertIsNotNone(kwargs['stdout'])
                runs = output / 'runs'
                runs.mkdir()
                make_run(runs, '2026-10-27_1200_abc', [('猫', 10)])
                return subprocess.CompletedProcess(command, 0)
            with patch('scripts.check_corpus.subprocess.run', side_effect=fake_run), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(check_main(['--config', str(config), '--output-dir', str(output)]), 0)
            self.assertEqual(len(list(output.glob('tango-check-*.log'))), 1)

    def test_config_round_trip_preserves_resolved_settings(self):
        config = resolve_config()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'config.toml'
            path.write_text(config_toml(config), encoding='utf8')
            self.assertEqual(resolve_config(path), config)


if __name__ == '__main__':
    unittest.main()
