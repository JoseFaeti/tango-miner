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

    def test_help_is_visible_without_discovering_tests(self):
        with tempfile.TemporaryDirectory() as tmp:
            marker = Path(tmp) / "imported"
            (Path(tmp) / "test_fixture.py").write_text(f"from pathlib import Path\nPath({str(marker)!r}).touch()\n", encoding="utf-8")
            result = subprocess.run([sys.executable, str(RUNNER), "discover", "-s", tmp, "--help"], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0)
            self.assertIn("usage:", result.stdout.lower())
            self.assertFalse(marker.exists())

    def test_standalone_script_help_does_not_create_output(self):
        root = RUNNER.parent
        with tempfile.TemporaryDirectory() as tmp:
            for name in ("check_jmdict.py", "jmdict_xml_test.py", "sudachipy_test.py"):
                with self.subTest(script=name):
                    result = subprocess.run([sys.executable, str(root / name), "--help"], cwd=tmp, capture_output=True, text=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn("usage:", result.stdout.lower())
                    self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_invalid_arguments_are_visible(self):
        result = subprocess.run([sys.executable, str(RUNNER), "--invalid-option"], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("error", result.stderr.lower())
