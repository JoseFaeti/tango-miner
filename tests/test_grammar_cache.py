import tempfile
from dataclasses import replace
import unittest
from pathlib import Path
from unittest.mock import patch

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep, GrammarPositionStep, AddGrammarCardsStep
from src.Pipeline import Pipeline
from src.StageCache import StageCache
from src.TokenizedSentence import TokenizedSentence


def fixture(text='読んでいる。'):
    tokens = [dict(surface=s, lemma=l, base_form=l, reading=s, pos=p) for s, l, p in [
        ('読ん', '読む', ('動詞', '一般')), ('で', 'で', ('助詞', '接続助詞')),
        ('いる', 'いる', ('動詞', '非自立可能')), ('。', '。', ('補助記号', '句点'))]]
    return [TokenizedSentence(text, 'source', Path('source.txt'), tokens)]


class GrammarCacheTests(unittest.TestCase):
    def test_cache_restores_state_and_downstream_behavior(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = StageCache(tmp, {})
            first = GrammarStep()
            Pipeline([first], cache=cache).run(Artifact(fixture()))
            second = GrammarStep()
            with patch.object(second, 'process', side_effect=AssertionError('cache miss')):
                pipe = Pipeline([second], cache=cache)
                pipe.run(Artifact(fixture()))
            self.assertEqual(pipe.stage_records[0]['cache']['hits'], 1)
            self.assertEqual(first.occurrences, second.occurrences)
            self.assertEqual(first.spans, second.spans)
            self.assertEqual(first.matches, second.matches)
            first_input = Artifact(fixture())
            second_input = Artifact(fixture())
            ExcludeGrammarTokensStep(first).process(first_input)
            ExcludeGrammarTokensStep(second).process(second_input)
            self.assertEqual(first_input.data, second_input.data)
            GrammarPositionStep(first).process(first_input)
            GrammarPositionStep(second).process(second_input)
            first_words, second_words = {}, {}
            AddGrammarCardsStep(first).process(Artifact(first_words))
            AddGrammarCardsStep(second).process(Artifact(second_words))
            self.assertEqual(first_words, second_words)

    def test_progress_reports_cache_preparation_and_matching_or_hit(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = StageCache(tmp, {})
            sources = fixture() * 5
            messages = []
            def report(step, current, total, **kwargs):
                messages.append((current, total, kwargs['additional_text']))
            Pipeline([GrammarStep()], cache=cache, on_progress=report).run(Artifact(sources))
            self.assertTrue(any(message == 'Preparing cache key' and current > 0 and total == 5
                                for current, total, message in messages))
            self.assertTrue(any(message == 'Hashing cache key' for _, _, message in messages))
            self.assertTrue(any(message == 'Checking grammar cache' for _, _, message in messages))
            self.assertTrue(any(message == 'Matching grammar' and current > 0
                                for current, _, message in messages))
            self.assertTrue(any(message == 'Saving grammar cache' for _, _, message in messages))
            messages.clear()
            Pipeline([GrammarStep()], cache=cache, on_progress=report).run(Artifact(sources))
            self.assertTrue(any('Cache hit' in message for _, _, message in messages))
            self.assertFalse(any(message == 'Matching grammar' for _, _, message in messages))

    def test_invalidation_refresh_and_corruption(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = StageCache(tmp, {})
            step = GrammarStep()
            cache.process(step, Artifact(fixture()))
            self.assertEqual(step.cache_stats['misses'], 1)
            cache.process(step, Artifact(fixture()))
            self.assertEqual(step.cache_stats['hits'], 1)
            step.max_examples = 2
            cache.process(step, Artifact(fixture()))
            self.assertEqual(step.cache_stats['misses'], 1)
            cache.refresh = {'all'}
            cache.process(step, Artifact(fixture()))
            self.assertEqual(step.cache_stats['refreshes'], 1)
            cache.refresh = set()
            for path in (Path(tmp) / 'stages' / 'grammar-v1').glob('*.json'):
                path.write_text('broken', encoding='utf8')
            cache.process(step, Artifact(fixture()))
            self.assertEqual(step.cache_stats['misses'], 1)
            cache.process(step, Artifact(fixture('別の文。')))
            self.assertEqual(step.cache_stats['misses'], 1)

    def test_input_identity_changes_invalidate_grammar_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = StageCache(tmp, {})
            original = fixture()
            step = GrammarStep()
            cache.process(step, Artifact(original))
            cache.process(step, Artifact(fixture()))
            self.assertEqual(step.cache_stats['hits'], 1)
            for change in ('text', 'tag', 'origin', 'lemma', 'pos_type'):
                with self.subTest(change=change):
                    changed = fixture()
                    source = changed[0]
                    if change == 'text':
                        changed[0] = replace(source, text=source.text + '。')
                    elif change == 'tag':
                        changed[0] = replace(source, tag='別のタグ')
                    elif change == 'origin':
                        changed[0] = replace(source, origin=Path('別.txt'))
                    elif change == 'lemma':
                        source.tokens[0]['lemma'] = '別'
                    else:
                        source.tokens[0]['pos'] = list(source.tokens[0]['pos'])
                    cache.process(step, Artifact(changed))
                    self.assertEqual(step.cache_stats['misses'], 1)

    def test_dictionary_and_source_fingerprints_invalidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            step = GrammarStep(prefer_expressions=True)
            first = StageCache(tmp, {'files': {'jmdict': {'sha256': 'a'}}})
            with patch('src.GrammarCache._fingerprint', return_value='code-a'):
                first.process(step, Artifact([]))
                first.process(step, Artifact([]))
                self.assertEqual(step.cache_stats['hits'], 1)
                second = StageCache(tmp, {'files': {'jmdict': {'sha256': 'b'}}})
                second.process(step, Artifact([]))
                self.assertEqual(step.cache_stats['misses'], 1)
            with patch('src.GrammarCache._fingerprint', return_value='code-b'):
                first.process(step, Artifact([]))
                self.assertEqual(step.cache_stats['misses'], 1)

    def test_dictionary_and_custom_rules_control_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            step = GrammarStep(prefer_expressions=True)
            cache = StageCache(tmp, {})
            with patch.object(step, 'process', return_value=Artifact([])):
                cache.process(step, Artifact([]))
            self.assertEqual(step.cache_stats['disabled_reason'], 'dictionary hash unavailable')
            step = GrammarStep(rules=())
            cache.process(step, Artifact([]))
            self.assertEqual(step.cache_stats['disabled_reason'], 'custom rules')


if __name__ == '__main__':
    unittest.main()
