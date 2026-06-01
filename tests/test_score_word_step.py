import unittest

from src.ProcessingStep import ProcessingStep
from src.ScoreWordStep import calculate_score, score_words, tag_diversity_factor

from tests.helpers import make_stats


class ScoreWordStepTests(unittest.TestCase):
    def test_calculate_score_prefers_early_frequent_words(self):
        early_frequent = calculate_score(index=1, max_index=100, frequency=50, max_frequency=50)
        late_rare = calculate_score(index=100, max_index=100, frequency=1, max_frequency=50)

        self.assertGreater(early_frequent, late_rare)
        self.assertLessEqual(early_frequent, 1.0)
        self.assertGreaterEqual(late_rare, 0.0)

    def test_calculate_score_handles_zero_max_index(self):
        self.assertEqual(
            calculate_score(index=0, max_index=0, frequency=0, max_frequency=0),
            0.3,
        )

    def test_tag_diversity_factor_ramps_to_saturation(self):
        self.assertAlmostEqual(tag_diversity_factor(1), 0.1)
        self.assertGreater(tag_diversity_factor(5), tag_diversity_factor(1))
        self.assertEqual(tag_diversity_factor(10), 1.0)
        self.assertEqual(tag_diversity_factor(20), 1.0)

    def test_score_words_mutates_scores_and_reports_progress(self):
        words = {
            "早い": make_stats(index=1, frequency=10, tags={"a", "b", "c"}),
            "遅い": make_stats(index=100, frequency=2, tags={"a"}),
        }
        events = []

        returned = score_words(words, progress_handler=lambda *args: events.append(args))

        self.assertIs(returned, words)
        self.assertGreater(words["早い"].score, words["遅い"].score)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[-1], (ProcessingStep.SCORING, 2, 2))

    def test_score_words_returns_empty_input_unchanged(self):
        words = {}

        self.assertIs(score_words(words), words)


if __name__ == "__main__":
    unittest.main()
