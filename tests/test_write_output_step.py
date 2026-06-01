import csv
import tempfile
import unittest
from pathlib import Path

from src.WriteOutputStep import write_final_file
from src.WordStats import Sentence

from tests.helpers import make_stats


class WriteOutputStepTests(unittest.TestCase):
    def test_write_final_file_sorts_by_score_and_splits_invalid_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "out.csv"
            valid_sentence = Sentence("猫がいる", "tag", "origin.txt", "猫")
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
            sentence_text = (Path(tmp) / "out.sentence.csv").read_text(encoding="utf-8")

        self.assertEqual([row[1] for row in rows], ["high", "low"])
        self.assertEqual(dropped[0][1], "drop")
        self.assertEqual(rows[0][6], "a b")
        self.assertIn("<span class='highlight'>猫</span>", rows[0][7])
        self.assertIn("猫がいる", sentence_text)
        self.assertEqual(events[-1][0], None)


if __name__ == "__main__":
    unittest.main()
