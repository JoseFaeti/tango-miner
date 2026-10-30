import unittest

from src.RunConfig import ScoringConfig
from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.steps.ScoreWordStep import ScoreWordStep
from src.steps.ProcessingStep import ProcessingStep
from src.steps.ScoreWordStep import calculate_score, score_words, tag_diversity_factor

from tests.helpers import make_stats


class ScoreWordStepTests(unittest.TestCase):
    def test_configured_weights_and_normalization_change_ranking(self):
        def words():
            return {
                "early": make_stats(index=0.0, frequency=2, tags={"a"}),
                "frequent": make_stats(index=0.9, frequency=100, tags={"a"}),
            }
        freq_words = words()
        position_words = words()
        Pipeline([ScoreWordStep(ScoringConfig(frequency_weight=1, position_weight=0, max_score=100))]).run(Artifact(freq_words))
        Pipeline([ScoreWordStep(ScoringConfig(frequency_weight=0, position_weight=1, max_score=100))]).run(Artifact(position_words))
        self.assertGreater(freq_words["frequent"].score, freq_words["early"].score)
        self.assertGreater(position_words["early"].score, position_words["frequent"].score)
        self.assertEqual(max(word.score for word in freq_words.values()), 100)
        self.assertEqual(max(word.score for word in position_words.values()), 100)

    def test_configured_tag_diversity_factors(self):
        self.assertEqual(tag_diversity_factor(1, saturation_tags=4, min_factor=0.4), 0.4)
        self.assertEqual(tag_diversity_factor(4, saturation_tags=4, min_factor=0.4), 1.0)
        self.assertAlmostEqual(calculate_score(0.5, 1, 1, w_freq=0, w_index=1, position_exponent=2), 0.25)

    def test_calculate_score_prefers_early_frequent_words(self):
        early_frequent = calculate_score(index=0.0, frequency=50, max_frequency=50)
        late_rare = calculate_score(index=1.0, frequency=1, max_frequency=50)

        self.assertGreater(early_frequent, late_rare)
        self.assertLessEqual(early_frequent, 1.0)
        self.assertGreaterEqual(late_rare, 0.0)

    def test_calculate_score_handles_zero_max_index(self):
        self.assertEqual(
            calculate_score(index=0, frequency=0, max_frequency=0),
            0.25,
        )

    def test_tag_diversity_factor_ramps_to_saturation(self):
        self.assertAlmostEqual(tag_diversity_factor(1), 0.1)
        self.assertGreater(tag_diversity_factor(5), tag_diversity_factor(1))
        self.assertEqual(tag_diversity_factor(10), 1.0)
        self.assertEqual(tag_diversity_factor(20), 1.0)

    def test_score_words_mutates_scores_and_reports_progress(self):
        words = {
            "早い": make_stats(index=0.0, frequency=10, tags={"a", "b", "c"}),
            "遅い": make_stats(index=1.0, frequency=2, tags={"a"}),
        }
        events = []

        returned = score_words(words, progress_handler=lambda *args: events.append(args))

        self.assertIs(returned, words)
        self.assertGreater(words["早い"].score, words["遅い"].score)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[-1], (2, 2))

    def test_score_words_returns_empty_input_unchanged(self):
        words = {}

        self.assertIs(score_words(words), words)


if __name__ == "__main__":
    unittest.main()
