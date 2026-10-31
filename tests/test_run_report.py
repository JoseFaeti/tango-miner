import tempfile
import unittest
from pathlib import Path

from src.RunReport import render_report, write_run_report


class RunReportTests(unittest.TestCase):
    def test_completed_report_distinguishes_recorded_counts_and_plan(self):
        summary = {"status": "completed", "duration_seconds": 12.5, "grammar_enabled": True,
                   "anki": {"status": "preview_only", "plan": {"add": 2, "modify": 1, "delete": 0}},
                   "stages": [
                       {"name": "AddDefinitionsStep", "valid_candidates": 42, "review_candidates": 3, "invalid_candidates": 1, "duration_seconds": 4, "status": "completed"},
                       {"name": "GrammarStep", "grammar_rules": {"a": {"occurrences": 7}, "b": {"occurrences": 2}}, "duration_seconds": 2, "status": "completed"},
                       {"name": "CompareOutputStep", "comparison": {"status": "compared", "baseline": "previous", "counts": {"added": 1, "removed": 2, "changed": 3}}},
                   ]}
        report = render_report(summary)
        for expected in ("COMPLETED", "Valid: 42", "Needs review: 3", "occurrences: 9", "Planned: add 2", "../previous/summary.json", "not Anki changes"):
            self.assertIn(expected, report)
        with tempfile.TemporaryDirectory() as tmp:
            write_run_report(tmp, summary)
            self.assertEqual((Path(tmp) / "report.md").read_text(encoding="utf-8"), report)

    def test_pipeline_writes_report_on_failure(self):
        from unittest.mock import patch
        from tango_miner import process_script

        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "config.toml"
            config.write_text("", encoding="utf-8")
            with patch("tango_miner.collect_dependencies", return_value={}), patch("tango_miner.Pipeline") as factory:
                factory.return_value.run.side_effect = RuntimeError("example failure")
                factory.return_value.stage_records = []
                factory.return_value.duration = 0.1
                with self.assertRaisesRegex(RuntimeError, "example failure"):
                    process_script(["--config", str(config), "--input", tmp])
            report, = (Path(tmp) / "runs").glob("*/report.md")
            self.assertIn("RuntimeError: example failure", report.read_text(encoding="utf-8"))

    def test_failed_partial_run_is_reported_without_inventing_counts(self):
        report = render_report({"status": "failed", "error": "RuntimeError: example", "grammar_enabled": False, "anki": {"status": "disabled"}, "stages": []})
        self.assertIn("FAILED", report)
        self.assertIn("RuntimeError: example", report)
        self.assertIn("Unavailable", report)
        self.assertIn("- Disabled", report)
        self.assertNotIn("Valid: 0", report)


if __name__ == "__main__":
    unittest.main()
