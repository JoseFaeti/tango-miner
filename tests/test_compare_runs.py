import contextlib
import csv
import io
import json
import tempfile
import unittest
from pathlib import Path

from scripts.compare_runs import compact_report, completed_runs, load_run, overview_report, sentence_coverage, report


def make_run(root, name, valid, dropped=(), status="completed"):
    path = root / name
    path.mkdir()
    for filename, data in [("summary.json", {"status": status, "anki": {"status": "preview_only"}}),
                           ("config.json", {"version": 1}), ("input-manifest.json", [])]:
        (path / filename).write_text(json.dumps(data), encoding="utf-8")
    for filename, entries in [("output.csv", valid), ("output.dropped.csv", dropped)]:
        with (path / filename).open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            for word, frequency in entries:
                writer.writerow([0, word, word, 0, frequency, "definition", "", "", "example"])
    return path


class CompareRunsTests(unittest.TestCase):
    def test_selection_and_report_include_transitions_and_frequency_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = make_run(root, "2026-10-27_1200_abc", [("猫", 4), ("旧", 5)], [("犬", 3)])
            second = make_run(root, "2026-10-27_1200_abc_2", [("猫", 7), ("犬", 3), ("新", 2)])
            make_run(root, "2026-10-27_1201_abc", [], status="running")
            self.assertEqual(completed_runs(root), [first, second])
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                report(load_run(first), load_run(second), ["猫"], 20)
            text = output.getvalue()
            self.assertIn("Added candidates (1): 新", text)
            self.assertIn("Removed candidates (1): 旧", text)
            self.assertIn("犬: dropped/review -> valid", text)
            self.assertIn("猫: 4 -> 7 (+3)", text)
            self.assertIn("Anki outcome", text)

    def test_compact_report_omits_full_definitions_and_bounds_lists(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = make_run(root, '2026-10-27_1200_a', [('猫', 3)])
            second = make_run(root, '2026-10-27_1201_b', [('犬', 4), ('鳥', 2)])
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                compact_report(load_run(first), load_run(second), ['猫'], 1)
            text = output.getvalue()
            self.assertIn('Added: 2; removed: 1', text)
            self.assertIn('猫: valid, frequency 3 -> absent', text)
            self.assertNotIn('definition', text)
            self.assertNotIn('example', text)

    def test_overview_reports_sentence_coverage_and_run_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = make_run(root, '2026-10-27_1200_a', [('猫', 4)], [('犬', 3)])
            second = make_run(root, '2026-10-27_1201_b', [('猫', 4)], [('犬', 3)])
            before, after = load_run(first), load_run(second)
            before['rows']['猫']['sentence_count'] = 'sentences: 0'
            after['rows']['猫']['sentence_count'] = 'sentences: 2'
            self.assertEqual(sentence_coverage(before)['valid'], {'0': 1})
            self.assertEqual(sentence_coverage(after)['dropped/review'], {'unavailable': 1})
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                overview_report(before, after, ['猫'], 0)
            text = output.getvalue()
            self.assertIn('Sentence counts', text)
            self.assertIn('"2": 1', text)
            self.assertIn('Anki status: preview_only', text)
            self.assertIn('Compatibility:', text)

    def test_missing_or_duplicate_outputs_fail_instead_of_silent_comparison(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = make_run(Path(tmp), "2026-10-27_1200_abc", [("猫", 1)], [("猫", 2)])
            with self.assertRaisesRegex(ValueError, "Duplicate candidate"):
                load_run(path)


if __name__ == "__main__":
    unittest.main()
