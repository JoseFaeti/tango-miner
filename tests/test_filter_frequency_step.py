from collections import OrderedDict
import unittest

from src.Artifact import Artifact
from src.FilterFrequencyStep import FilterFrequencyStep, filter_useful_words
from src.ProcessingStep import ProcessingStep

from tests.helpers import make_stats


class FilterFrequencyStepTests(unittest.TestCase):
    def test_filter_keeps_words_at_or_above_min_frequency(self):
        words = OrderedDict(
            [
                ("low", make_stats(frequency=1)),
                ("edge", make_stats(frequency=4)),
                ("high", make_stats(frequency=10)),
            ]
        )

        kept = filter_useful_words(words, min_frequency=4, keep_percent=100)

        self.assertEqual(list(kept.keys()), ["edge", "high"])

    def test_filter_applies_percentile_threshold_when_higher_than_minimum(self):
        words = OrderedDict(
            (str(i), make_stats(frequency=i))
            for i in range(1, 101)
        )

        kept = filter_useful_words(words, min_frequency=1, keep_percent=50)

        self.assertEqual(min(stats.frequency for stats in kept.values()), 51)
        self.assertEqual(len(kept), 50)

    def test_filter_empty_input_returns_empty_ordered_dict(self):
        kept = filter_useful_words(OrderedDict(), min_frequency=4)

        self.assertIsInstance(kept, OrderedDict)
        self.assertEqual(kept, OrderedDict())

    def test_step_preserves_sentences_and_reports_progress(self):
        words = OrderedDict(
            [
                ("keep", make_stats(frequency=5)),
                ("drop", make_stats(frequency=1)),
            ]
        )
        sentences = ["sentence"]
        step = FilterFrequencyStep(min_frequency=4)
        events = []
        step._progress_handler = lambda event: events.append(event)

        artifact = step.process(Artifact(words, sentences=sentences))

        self.assertEqual(list(artifact.data.keys()), ["keep"])
        self.assertEqual(artifact.sentences, sentences)
        self.assertEqual(events[-1].step, ProcessingStep.FILTERING)


if __name__ == "__main__":
    unittest.main()
