"""Check the module bridges against their original script entry points."""
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class AgentCommandTests(unittest.TestCase):
    def test_all_commands_support_help_and_silent(self):
        commands = sorted(p.stem for p in (ROOT / 'agent_commands').glob('*.py')
                          if p.stem != 'runner')
        self.assertEqual(len(commands), 14)
        for command in commands:
            with self.subTest(command=command):
                base = [sys.executable, '-m', f'tango_miner.{command}']
                help_result = subprocess.run(base + ['--help'], cwd=ROOT, capture_output=True)
                self.assertEqual(help_result.returncode, 0, help_result.stderr)
                self.assertIn(b'--silent', help_result.stdout)
                self.assertLessEqual(len(help_result.stdout.splitlines()), 40)
                silent = subprocess.run(base + ['--silent', '--help'], cwd=ROOT, capture_output=True)
                self.assertEqual((silent.returncode, silent.stdout, silent.stderr), (0, b'', b''))

    def test_silent_preserves_failure_status(self):
        result = subprocess.run([sys.executable, '-m', 'tango_miner.query_jsonl',
                                 '--silent', 'missing.jsonl'], cwd=ROOT, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual((result.stdout, result.stderr), (b'', b''))

    def test_silent_preserves_file_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            sample = Path(tmp) / 'sample.jsonl'
            output = Path(tmp) / 'labels.jsonl'
            sample.write_text('{"id":"1","candidate":"猫","sentence":"猫"}\n', encoding='utf-8')
            result = subprocess.run([sys.executable, '-m', 'tango_miner.evaluate_review',
                                     'label-template', '--sample', str(sample), '--output',
                                     str(output), '--silent'], cwd=ROOT, capture_output=True)
            self.assertEqual((result.returncode, result.stdout, result.stderr), (0, b'', b''))
            self.assertIn('猫', output.read_text(encoding='utf-8'))

    def test_module_matches_script(self):
        for command, args in (
            ('next_grammar', []),
            ('inspect_run', ['--help']),
            ('query_jsonl', ['--invalid-option']),
        ):
            with self.subTest(command=command):
                script = subprocess.run(
                    [sys.executable, f'scripts/{command}.py', *args],
                    cwd=ROOT, capture_output=True,
                )
                module = subprocess.run(
                    [sys.executable, '-m', f'tango_miner.{command}', *args],
                    cwd=ROOT, capture_output=True,
                )
                self.assertEqual(module.returncode, script.returncode)
                # argparse includes the executable path in usage text.
                if command == 'next_grammar':
                    self.assertEqual(module.stdout, script.stdout)
                    self.assertEqual(module.stderr, script.stderr)


if __name__ == '__main__':
    unittest.main()
