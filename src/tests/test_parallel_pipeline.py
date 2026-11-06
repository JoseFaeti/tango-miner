import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET
from concurrent.futures import ProcessPoolExecutor

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.Pipeline import Pipeline
from src.Parallel import ordered_map
from src.StageCache import StageCache
from src.TokenizedSentence import TokenizedSentence
from src.steps.MergeTokensStep.index import MergeTokensStep


def token(surface, lemma, pos):
    return {'surface': surface, 'lemma': lemma, 'base_form': lemma, 'reading': surface, 'pos': pos}


def source(prefix, tag):
    tokens = [token(prefix, prefix, ('名詞', '普通名詞')),
              token('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
              token('で', 'で', ('助詞', '接続助詞')),
              token('いる', 'いる', ('動詞', '非自立可能', '*', '*', '一段', '終止形-一般')),
              token('。', '。', ('補助記号', '句点'))]
    return TokenizedSentence(prefix + '読んでいる。', tag, Path('source.txt'), tokens)


class ParallelPipelineTests(unittest.TestCase):
    def test_parallel_grammar_merge_and_cache_match_serial(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dictionary = root / 'dictionary.xml'
            dictionary.write_text('''<JMdict>
                <entry><ent_seq>1</ent_seq><k_ele><keb>語彙</keb></k_ele>
                  <r_ele><reb>ごい</reb></r_ele><sense><pos>noun (common)</pos><gloss>vocabulary</gloss></sense></entry>
                <entry><ent_seq>2</ent_seq><r_ele><reb>から</reb></r_ele>
                  <sense><pos>noun (common)</pos><gloss>shell</gloss></sense></entry>
                </JMdict>''', encoding='utf-8')
            a, b, c = source('私は本を', 'a'), source('君は本を', 'b'), source('皆は本を', 'c')
            lexical = TokenizedSentence('語彙から。', 'lexical', Path('source.txt'), [
                token('語', '語', ('名詞', '普通名詞')),
                token('彙', '彙', ('名詞', '普通名詞')),
                token('か', 'か', ('名詞', '普通名詞')),
                token('ら', 'ら', ('名詞', '普通名詞')),
                token('。', '。', ('補助記号', '句点'))])
            inputs = [a, b] * 500 + [a, b, c, lexical] * 251

            def run(workers, cache=None, diagnostics=None):
                grammar = GrammarStep(max_examples=3, dictionary_path=dictionary,
                                      prefer_expressions=True, profiling=True)
                merge = MergeTokensStep(dictionary_path=dictionary, diagnostics_path=diagnostics)
                steps = [grammar, ExcludeGrammarTokensStep(grammar), merge]
                pipeline = Pipeline(steps, workers=workers, cache=cache, profiling=True)
                result = pipeline.run(Artifact(inputs))
                return grammar, result, merge, pipeline

            serial, serial_result, _, _ = run(1, diagnostics=root / 'serial.jsonl')
            parallel, parallel_result, _, pipeline = run(2, diagnostics=root / 'parallel.jsonl')
            self.assertEqual(serial.occurrences, parallel.occurrences)
            self.assertEqual(serial.spans, parallel.spans)
            self.assertEqual(serial.matches, parallel.matches)
            self.assertEqual(serial_result.data, parallel_result.data)
            self.assertEqual((root / 'serial.jsonl').read_bytes(), (root / 'parallel.jsonl').read_bytes())
            self.assertGreater((root / 'serial.jsonl').stat().st_size, 0)
            self.assertIn('語彙', [item['surface'] for item in parallel_result.data[-1].tokens])
            self.assertIs(parallel_result.data[0].tokens[0], inputs[0].tokens[0])
            self.assertIn('parallel_wall', pipeline.performance['1']['entries_seconds'])
            self.assertTrue(all(getattr(step, '_parallel_executor', None) is None for step in pipeline.steps))
            self.assertEqual([e.text for e in parallel.occurrences['te_iru'].examples],
                             [a.text, b.text, c.text])

            dependencies = {'files': {'jmdict': {'sha256': hashlib.sha256(dictionary.read_bytes()).hexdigest()}}}
            cache_dir = root / 'cache'
            cache = StageCache(cache_dir, dependencies)
            first, first_result, _, _ = run(2, cache=cache)
            self.assertEqual(first.occurrences, serial.occurrences)
            self.assertEqual(first_result.data, serial_result.data)
            self.assertEqual(len(list((cache_dir / 'shared-jmdict-v1').glob('*.bin'))), 1)
            warm_cache = StageCache(cache_dir, dependencies)
            second, second_result, _, warm = run(1, cache=warm_cache)
            self.assertEqual(second.occurrences, serial.occurrences)
            self.assertEqual(second_result.data, serial_result.data)
            self.assertEqual(warm.stage_records[0]['cache']['hits'], 1)
            self.assertEqual(warm.stage_records[2]['cache']['hits'], 1)

    def test_parallel_cache_across_small_sources_and_duplicate_group_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dictionary = root / 'dictionary.xml'
            dictionary.write_text('<JMdict/>', encoding='utf-8')
            digest = hashlib.sha256(dictionary.read_bytes()).hexdigest()
            dependencies = {'files': {'jmdict': {'sha256': digest}}}
            groups = []
            for i in range(6):
                item = source('私は本を', str(i))
                item = TokenizedSentence(item.text, item.tag, Path(f'source-{i}'), item.tokens)
                groups.extend([item] * 100)

            def run(items, workers, cache_dir):
                cache = StageCache(cache_dir, dependencies)
                result = Pipeline([MergeTokensStep(dictionary_path=dictionary)],
                                  workers=workers, cache=cache).run(Artifact(items))
                return result, cache.stats

            serial, serial_stats = run(groups, 1, root / 'serial')
            parallel, parallel_stats = run(groups, 3, root / 'parallel')
            self.assertEqual(serial.data, parallel.data)
            self.assertEqual(serial_stats, parallel_stats)
            self.assertEqual(parallel_stats['misses'], 6)
            self.assertEqual(len(list((root / 'parallel' / 'shared-jmdict-v1').glob('*.bin'))), 1)
            repeated = groups[:100] * 2 + groups[100:200] * 2 + groups[:100] * 2
            serial, serial_stats = run(repeated, 1, root / 'serial-duplicate')
            parallel, parallel_stats = run(repeated, 3, root / 'parallel-duplicate')
            self.assertEqual(serial.data, parallel.data)
            self.assertEqual(serial_stats, parallel_stats)
            self.assertEqual(parallel_stats['hits'], 1)

    def test_uncacheable_tokens_fall_back_to_serial_processing(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dictionary = root / 'dictionary.xml'
            dictionary.write_text('<JMdict/>', encoding='utf-8')
            digest = hashlib.sha256(dictionary.read_bytes()).hexdigest()
            cache = StageCache(root, {'files': {'jmdict': {'sha256': digest}}})
            item = source('私は本を', 'tag')
            item.tokens[0]['custom'] = lambda: None
            grammar = GrammarStep(dictionary_path=dictionary)
            merge = MergeTokensStep(dictionary_path=dictionary)
            pipeline = Pipeline([grammar, merge], workers=2, cache=cache)
            output = pipeline.run(Artifact([item] * 512))
            self.assertEqual(len(output.data), 512)
            self.assertIn('disabled_reason', grammar.cache_stats)
            self.assertEqual(cache.stats['misses'], 1)

    def test_worker_failure_preserves_diagnostics_and_shuts_down(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            dictionary = root / 'broken.xml'
            dictionary.write_text('<JMdict>', encoding='utf-8')
            output = root / 'rejected.jsonl'
            output.write_text('previous', encoding='utf-8')
            stage = MergeTokensStep(dictionary_path=dictionary, diagnostics_path=output)
            pipeline = Pipeline([stage], workers=2)
            with self.assertRaises(ET.ParseError):
                pipeline.run(Artifact([source('私は本を', 'a')] * 2000))
            self.assertEqual(output.read_text(encoding='utf-8'), 'previous')
            self.assertIsNone(stage._parallel_executor)
            self.assertEqual(pipeline.stage_records[0]['status'], 'failed')

    def test_ordered_map_handles_uneven_batches(self):
        with ProcessPoolExecutor(max_workers=2) as executor:
            for length in range(12):
                values = list(range(-length, 0))
                with self.subTest(length=length):
                    self.assertEqual(list(ordered_map(executor, abs, iter(values), 3)),
                                     [abs(value) for value in values])

    def test_invalid_worker_counts_rejected(self):
        for workers in (0, -1, True):
            with self.subTest(workers=workers), self.assertRaises(ValueError):
                Pipeline([], workers=workers)


if __name__ == '__main__':
    unittest.main()
