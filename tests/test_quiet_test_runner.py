import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


RUNNER = Path(__file__).resolve().parents[1] / "scripts" / "run_tests.py"


class QuietTestRunnerTests(unittest.TestCase):
    def run_fixture(self, source):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "test_fixture.py").write_text(source, encoding="utf-8")
            return subprocess.run([sys.executable, str(RUNNER), "discover", "-s", tmp],
                                  capture_output=True, text=True)

    def test_success_suppresses_test_output(self):
        result = self.run_fixture("import unittest\nclass Case(unittest.TestCase):\n def test_ok(self):\n  print('noise')\n  import subprocess, sys\n  subprocess.run([sys.executable, '-c', 'print(123)'], check=True)\n")
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout + result.stderr, "")

    def test_failure_displays_diagnostics_and_returns_failure(self):
        result = self.run_fixture("import unittest\nclass Case(unittest.TestCase):\n def test_bad(self):\n  print('context')\n  self.fail('broken')\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("broken", result.stderr)
        self.assertIn("context", result.stderr)

    def test_invalid_arguments_are_visible(self):
        result = subprocess.run([sys.executable, str(RUNNER), "--invalid-option"], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("error", result.stderr.lower())
