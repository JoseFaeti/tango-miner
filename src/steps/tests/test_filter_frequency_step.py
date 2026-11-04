from collections import OrderedDict
import unittest

from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.steps.FilterFrequencyStep import FilterFrequencyStep, filter_useful_words
from src.steps.ProcessingStep import ProcessingStep

from tests.helpers import make_stats
from src.TokenProvenance import TokenComponent


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

    def test_step_uses_configured_per_source_threshold(self):
        words = OrderedDict([("word", make_stats(frequency=2, tags={"a", "b"}))])
        kept = Pipeline([FilterFrequencyStep(1, keep_percent=100, min_frequency_per_source=1.0)]).run(Artifact(words))
        dropped = Pipeline([FilterFrequencyStep(1, keep_percent=100, min_frequency_per_source=1.1)]).run(Artifact(words))
        self.assertIn("word", kept.data)
        self.assertNotIn("word", dropped.data)

    def test_low_coverage_dictionary_expression_is_kept_for_review(self):
        expression = make_stats(frequency=4, tags={'a', 'b', 'c', 'd'})
        expression.dictionary_expression_occurrences = 4
        lexical = make_stats(frequency=4, tags={'a', 'b', 'c', 'd'})
        below_threshold = make_stats(frequency=3, tags={'a'})
        below_threshold.dictionary_expression_occurrences = 3
        words = OrderedDict([('expression', expression), ('lexical', lexical),
                             ('below', below_threshold)])
        result = filter_useful_words(words, min_frequency=4, keep_percent=100)
        self.assertEqual(list(result), ['expression'])
        self.assertTrue(expression.coverage_review)

    def test_low_frequency_inflection_is_reviewed_only_with_supported_base(self):
        def component(surface, lemma, pos):
            return TokenComponent(surface, lemma, lemma, surface, (pos, '*'))
        inflected = make_stats(frequency=3, tags={'a'})
        inflected.dictionary_expression_occurrences = 3
        inflected.component_counts = {(component('目', '目', '名詞'),
                                       component('に', 'に', '助詞'),
                                       component('見え', '見える', '動詞'),
                                       component('て', 'て', '助詞')): 3}
        base = make_stats(frequency=8, tags={'a', 'b'})
        base.dictionary_expression_occurrences = 8
        words = OrderedDict([('目に見えて', inflected), ('目に見える', base)])
        kept = filter_useful_words(words, min_frequency=4, keep_percent=100)
        self.assertIn('目に見えて', kept)
        self.assertTrue(inflected.coverage_review)
        base.dictionary_expression_occurrences = 0
        self.assertNotIn('目に見えて', filter_useful_words(words, min_frequency=4, keep_percent=100))

    def test_excludes_vocal_sounds_but_preserves_words_and_ambiguous_analyses(self):
        from src.steps.TokenizeStep import tokenize
        text = ['はぁはぁ、疲れた。', 'ははは、面白いね。', 'ぐふふ、やったぞ。',
                '母は笑う。', '彼はすきっと爽やかな表情だ。']
        words, _ = tokenize(text, tag='fixture')
        for stats in words.values():
            stats.frequency = 5
        kept = filter_useful_words(words, min_frequency=1, keep_percent=100)
        for sound in ['はぁはぁ', 'ぐふふ']:
            self.assertIn(sound, words)
            self.assertNotIn(sound, kept)
        self.assertIn('母', kept)
        self.assertIn('笑う', kept)
        self.assertIn('すきっと', kept)
        laughter = make_stats(frequency=5, tags={'fixture'})
        laughter.component_counts = {(TokenComponent('ははは', 'ははは', 'ははは', 'ははは', ('感動詞',)),): 1}
        self.assertNotIn('ははは', filter_useful_words(OrderedDict([('ははは', laughter)]), min_frequency=1))
        split = make_stats(frequency=5, tags={'fixture'})
        split.component_counts = {
            (TokenComponent('は', 'は', 'は', 'は', ('助詞',)),
             TokenComponent('はは', 'はは', 'はは', 'はは', ('感動詞',))): 3,
            (TokenComponent('はは', 'はは', 'はは', 'はは', ('副詞',)),): 1,
        }
        self.assertNotIn('ははは', filter_useful_words(OrderedDict([('ははは', split)]), min_frequency=1))
        panting = make_stats(frequency=5, tags={'fixture'})
        panting.component_counts = {
            (TokenComponent('はぁ', 'はぁ', 'はぁ', 'はぁ', ('感動詞',)),
             TokenComponent('はぁ', 'はぁ', 'はぁ', 'はぁ', ('感動詞',))): 1,
            (TokenComponent('はぁ', 'はぁ', 'はぁ', 'はぁ', ('副詞',)),): 1,
        }
        self.assertNotIn('はぁはぁ', filter_useful_words(OrderedDict([('はぁはぁ', panting)]), min_frequency=1))
        ambiguous = make_stats(frequency=5, tags={'fixture'})
        ambiguous.component_counts = {(TokenComponent('ははは', 'ははは', 'ははは', 'ははは', ('名詞',)),): 1}
        self.assertIn('ははは', filter_useful_words(OrderedDict([('ははは', ambiguous)]), min_frequency=1))

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
        artifact = Pipeline([step], on_progress=lambda *args, **kwargs: events.append((args, kwargs))).run(
            Artifact(words, sentences=sentences),
        )

        self.assertEqual(list(artifact.data.keys()), ["keep"])
        self.assertEqual(artifact.sentences, sentences)
        self.assertEqual(events[-1][0][0], ProcessingStep.FILTERING)


if __name__ == "__main__":
    unittest.main()
