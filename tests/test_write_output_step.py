import csv
import json
import tempfile
import unittest
from pathlib import Path

from src.steps.WriteOutputStep import write_final_file, write_candidate_diagnostics
from src.WordStats import Sentence

from tests.helpers import make_stats


class WriteOutputStepTests(unittest.TestCase):
    def test_highlight_uses_exact_occurrence_and_escapes_html(self):
        sentence = Sentence("変わったら わった & <", "tag", "origin", "わっ", 1, (6, 8))
        self.assertIn("変わったら <span class='highlight'>わっ</span>た &amp; &lt;", sentence.to_html())
        invalid = Sentence("変わったら", "tag", "origin", "わっ", 1, (-1, -1))
        self.assertNotIn("highlight", invalid.to_html())

    def test_unresolved_candidate_is_written_for_review_with_examples(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            stats = make_stats(sentences=[Sentence("会えるかな", "tag", "origin", "会える", 1)])
            stats.needs_review = True
            write_final_file({"会える": stats}, path)
            with path.open(encoding="utf-8", newline="") as stream:
                self.assertEqual(list(csv.reader(stream)), [])
            with (Path(tmp) / "out.dropped.csv").open(encoding="utf-8", newline="") as stream:
                self.assertEqual(list(csv.reader(stream))[0][1], "会える")
            with (Path(tmp) / "out.sentence.csv").open(encoding="utf-8", newline="") as stream:
                self.assertEqual(list(csv.reader(stream))[1][0], "review")

    def test_diagnostics_explain_coverage_review(self):
        stats = make_stats(definition='meaning')
        stats.needs_review = True
        stats.coverage_review = True
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'candidates.jsonl'
            write_candidate_diagnostics({'expression': stats}, path)
            row = json.loads(path.read_text(encoding='utf-8'))
        self.assertEqual(row['review_reasons'], ['expression_frequency_or_source_coverage'])

    def test_write_final_file_sorts_by_score_and_splits_invalid_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.csv"
            valid_sentence = Sentence("猫がいる", "tag", "origin.txt", "猫", 100)
            words = {
                "low": make_stats(score=10, invalid=False),
                "drop": make_stats(score=999, invalid=True),
                "high": make_stats(score=100, invalid=False, tags={"b", "a"}, sentences=[valid_sentence]),
            }
            events = []

            write_final_file(words, output, progress_handler=lambda *args: events.append(args))

            with output.open(encoding="utf-8", newline="") as f:
                rows = list(csv.reader(f))
            with (Path(tmp) / "out.dropped.csv").open(encoding="utf-8", newline="") as f:
                dropped = list(csv.reader(f))
            with (Path(tmp) / "out.sentence.csv").open(encoding="utf-8", newline="") as f:
                sentence_rows = list(csv.reader(f))

        self.assertEqual([row[1] for row in rows], ["high", "low"])
        self.assertEqual(dropped[0][1], "drop")
        self.assertEqual(rows[0][6], "a b")
        self.assertIn("<span class='highlight'>猫</span>", rows[0][8])
        self.assertEqual(
            sentence_rows[0],
            ["status", "word", "score", "sentence", "tag", "origin", "surface_form"],
        )
        self.assertEqual(sentence_rows[1][:4], ["kept", "high", "100", "猫がいる"])
        self.assertEqual(events[-1][0], 1)


if __name__ == "__main__":
    unittest.main()
