"""Check the module bridges against their original script entry points."""
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


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

    def test_inspect_anki_plan_delegates_to_script(self):
        with tempfile.TemporaryDirectory() as tmp:
            plan = Path(tmp) / 'anki-plan.json'
            plan.write_text(json.dumps({
                'add': [{'expression': '犬'}],
                'modify': [{'expression': '猫', 'fields': {
                    'Meaning': {'before': 'old', 'after': 'new'}},
                    'tags_added': ['review']}],
                'delete': [],
            }, ensure_ascii=False), encoding='utf-8')
            args = [str(plan), '--examples', '1']
            script = subprocess.run([sys.executable, 'scripts/inspect_anki_plan.py', *args],
                                    cwd=ROOT, capture_output=True)
            module = subprocess.run([sys.executable, '-m', 'tango_miner.inspect_anki_plan', *args],
                                    cwd=ROOT, capture_output=True)
            self.assertEqual(script.returncode, 0, script.stderr)
            self.assertEqual((module.returncode, module.stdout, module.stderr),
                             (script.returncode, script.stdout, script.stderr))
            self.assertIn(b'Added: 1; modified: 1; deleted: 0', module.stdout)
            self.assertIn(b'Meaning: 1', module.stdout)

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
