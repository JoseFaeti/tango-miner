import tempfile
import json
import hashlib
import unittest
from pathlib import Path
from unittest.mock import patch

from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.StageCache import StageCache
from src.TokenProvenance import TokenComponent
from src.TokenizedSentence import TokenizedSentence
from src.steps.MergeTokensStep import MergeTokensStep
from src.steps.TokenizeStep import RawTokenizeStep
from src.steps.BuildWordStatsStep import BuildWordStatsStep


def sentence(name, text):
    return TokenizedSentence(text, 'tag', Path(name), [{'surface': text, 'pos': ('noun',), 'components': (TokenComponent(text, text, text, text, ('noun',)),)}])


class StageCacheTests(unittest.TestCase):
    def test_per_source_hits_refresh_and_dependency_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            deps = {'files': {'jmdict': {'sha256': 'dictionary-a'}}}
            inputs = [sentence('a', '猫'), sentence('b', '犬')]
            step = MergeTokensStep(max_lookahead=4)
            def compute(artifact):
                return Artifact(artifact.data)
            with patch.object(step, 'process', side_effect=compute) as process:
                cache = StageCache(tmp, deps)
                first = cache.process(step, Artifact(inputs))
                self.assertEqual(first.data, inputs)
                self.assertEqual(cache.stats['misses'], 2)
                second = cache.process(step, Artifact(inputs))
                self.assertEqual(second.data, inputs)
                self.assertEqual(cache.stats['hits'], 2)
                self.assertEqual(cache.stats['hit_streams'], 2)
                self.assertEqual(cache.stats['unit'], 'source groups')
                self.assertEqual(process.call_count, 2)
                cache.process(step, Artifact([inputs[0], sentence('b', '鳥')]))
                self.assertEqual((cache.stats['hits'], cache.stats['misses']), (1, 1))
                cache.refresh = {'merge'}
                cache.process(step, Artifact(inputs))
                self.assertEqual(cache.stats['refreshes'], 2)
                cache.refresh = set()
                step.max_lookahead = 5
                cache.process(step, Artifact(inputs))
                self.assertEqual(cache.stats['misses'], 2)
                other = StageCache(tmp, {'files': {'jmdict': {'sha256': 'dictionary-b'}}})
                other.process(step, Artifact(inputs))
                self.assertEqual(other.stats['misses'], 2)

    def test_cached_merge_reuses_unchanged_tokens_and_restores_changed_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = StageCache(tmp, {'files': {'jmdict': {'sha256': 'x'}}})
            original = [sentence('a', '猫'), sentence('a', '犬')]
            step = MergeTokensStep()
            def compute(artifact):
                first, second = artifact.data
                changed = dict(second.tokens[0], lemma='修正', analysis_rule='test')
                return Artifact([first, TokenizedSentence(second.text, second.tag, second.origin, [changed])])
            with patch.object(step, 'process', side_effect=compute) as process:
                cold = cache.process(step, Artifact(original))
                warm = cache.process(step, Artifact(original))
            self.assertEqual(warm.data, cold.data)
            self.assertEqual(warm.data[1].tokens[0]['lemma'], '修正')
            self.assertEqual(warm.data[1].tokens[0]['components'], original[1].tokens[0]['components'])
            self.assertEqual(cache.stats['hit_streams'], 2)
            self.assertEqual(process.call_count, 1)

    def test_corruption_is_a_miss_and_diagnostics_bypass_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = StageCache(tmp, {'files': {'jmdict': {'sha256': 'x'}}})
            step = MergeTokensStep()
            inputs = [sentence('a', '猫')]
            with patch.object(step, 'process', side_effect=lambda artifact: Artifact(artifact.data)) as process:
                cache.process(step, Artifact(inputs))
                path, = (Path(tmp) / 'stages' / 'merge-v2').glob('*.json')
                path.write_text('bad', encoding='utf8')
                cache.process(step, Artifact(inputs))
                self.assertEqual(cache.stats['misses'], 1)
                step.diagnostics_path = Path(tmp) / 'diagnostics.jsonl'
                cache.process(step, Artifact(inputs))
                self.assertEqual(cache.stats['disabled_reason'], 'diagnostics')
                self.assertEqual(process.call_count, 3)

    def test_invalid_token_reference_is_recomputed(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = StageCache(tmp, {'files': {'jmdict': {'sha256': 'x'}}})
            step = MergeTokensStep()
            inputs = [sentence('a', '猫')]
            with patch.object(step, 'process', side_effect=lambda a: Artifact(a.data)) as process:
                cache.process(step, Artifact(inputs))
                path, = (Path(tmp) / 'stages' / 'merge-v2').glob('*.json')
                entry = json.loads(path.read_text(encoding='utf8'))
                entry['data'] = [[-1]]
                encoded = json.dumps(entry['data'], ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf8')
                entry['checksum'] = hashlib.sha256(encoded).hexdigest()
                path.write_text(json.dumps(entry, ensure_ascii=False), encoding='utf8')
                result = cache.process(step, Artifact(inputs))
                self.assertEqual(result.data, inputs)
                self.assertEqual(cache.stats['miss_streams'], 1)
                self.assertEqual(process.call_count, 2)

    def test_pipeline_reports_cache_and_preserves_tmpdir(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = StageCache(tmp, {'files': {'jmdict': {'sha256': 'x'}}})
            step = MergeTokensStep()
            with patch.object(step, 'process', side_effect=lambda a: Artifact(a.data)):
                pipeline = Pipeline([step], cache=cache)
                for _ in range(2):
                    result = pipeline.run(Artifact([sentence('a', '猫')], tmpdir=Path(tmp)))
                self.assertEqual(result.tmpdir, Path(tmp))
                self.assertEqual(pipeline.stage_records[0]['cache']['hits'], 1)

    def test_real_merge_and_candidate_evidence_match_uncached(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = Pipeline([RawTokenizeStep()]).run(Artifact([(Path('fixture.txt'), ['おまもりをください。'])]))
            step = MergeTokensStep()
            uncached = Pipeline([step]).run(raw)
            cache = StageCache(tmp, {'files': {'jmdict': {'sha256': 'fixture'}}})
            pipeline = Pipeline([step], cache=cache)
            pipeline.run(raw)
            cached = pipeline.run(raw)
            self.assertEqual(cached.data, uncached.data)
            self.assertEqual(cache.stats['hits'], 1)
            expected = Pipeline([BuildWordStatsStep()]).run(uncached)
            actual = Pipeline([BuildWordStatsStep()]).run(cached)
            self.assertEqual(actual.data, expected.data)
            self.assertEqual(actual.sentences, expected.sentences)
            self.assertIn('おまもり', actual.data)
            self.assertTrue(actual.data['おまもり'].component_counts)

    def test_cli_all_refresh_forces_tokenizer_and_merge(self):
        from tango_miner import process_script
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / 'config.toml'
            config.write_text('', encoding='utf8')
            with patch('tango_miner.collect_dependencies', return_value={}), patch('tango_miner.Pipeline') as factory:
                factory.return_value.duration = 0
                process_script(['--config', str(config), '--input', tmp, '--refresh-cache', 'all'])
                steps = factory.call_args.kwargs['steps']
                self.assertTrue(next(s for s in steps if isinstance(s, RawTokenizeStep)).force_retokenize)
                self.assertEqual(factory.call_args.kwargs['cache'].refresh, {'all'})

    def test_write_failure_is_reported_and_output_remains_usable(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = StageCache(tmp, {'files': {'jmdict': {'sha256': 'x'}}})
            step = MergeTokensStep()
            inputs = [sentence('a', '猫')]
            with patch.object(step, 'process', side_effect=lambda a: Artifact(a.data)), patch.object(cache, '_save', return_value=False):
                result = cache.process(step, Artifact(inputs))
            self.assertEqual(result.data, inputs)
            self.assertEqual(cache.stats['write_failures'], 1)

    def test_unavailable_dictionary_disables_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = StageCache(tmp, {})
            step = MergeTokensStep()
            with patch.object(step, 'process', side_effect=lambda a: Artifact(a.data)):
                cache.process(step, Artifact([sentence('a', '猫')]))
            self.assertEqual(cache.stats['disabled_reason'], 'dictionary hash unavailable')


if __name__ == '__main__':
    unittest.main()
