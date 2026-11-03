"""Check the module bridges against their original script entry points."""
import subprocess
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class AgentCommandTests(unittest.TestCase):
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
