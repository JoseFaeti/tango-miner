import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from src.Artifact import Artifact
from src.Pipeline import Pipeline
from src.PipelineStep import PipelineStep
from src.RunConfig import resolve_config
from src.Grammar import GrammarStep
from src.StageCache import StageCache
from src.RunArchive import write_performance, write_stage_record
from tests.test_grammar_cache import fixture


class PerformanceTests(unittest.TestCase):
    def test_pipeline_profiles_each_step_and_preserves_failure(self):
        class Step(PipelineStep):
            def process(self, artifact):
                with self.performance_phase("work"):
                    pass
                return artifact

        class Broken(Step):
            def process(self, artifact):
                with self.performance_phase('partial'):
                    raise ValueError('failure')

        pipeline = Pipeline([Step(), Broken()], profiling=True)
        with self.assertRaisesRegex(ValueError, 'failure'):
            pipeline.run(Artifact([]))
        self.assertEqual(set(pipeline.performance), {'1', '2'})
        self.assertEqual(pipeline.performance['1']['name'], 'Step')
        self.assertEqual(pipeline.performance['2']['name'], 'Broken')
        self.assertGreaterEqual(pipeline.performance['1']['entries_seconds']['work'], 0)
        self.assertEqual(pipeline.performance['1']['entries_seconds']['total'], pipeline.stage_records[0]['duration_seconds'])
        self.assertGreaterEqual(pipeline.performance['2']['entries_seconds']['partial'], 0)
        self.assertEqual(pipeline.performance['2']['entries_seconds']['total'], pipeline.stage_records[1]['duration_seconds'])
        self.assertIn('duration_seconds', pipeline.stage_records[0])
        self.assertEqual(pipeline.stage_records[1]['status'], 'failed')
        disabled = Pipeline([Step()], profiling=False)
        disabled.run(Artifact([]))
        self.assertEqual(disabled.performance, {})

    def test_archive_includes_totals_and_interrupted_cache_key(self):
        with tempfile.TemporaryDirectory() as tmp:
            step = GrammarStep()
            cache = StageCache(tmp, {})
            def record(index, stage, data):
                write_stage_record(tmp, index, stage, data)
                if data['status'] != 'running':
                    write_performance(tmp, pipeline.performance)
            pipeline = Pipeline([step], cache=cache, on_stage_record=record, profiling=True)
            with patch('src.GrammarCache._input_key_data', side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    pipeline.run(Artifact(fixture()))
            data = json.loads((Path(tmp) / 'performance.json').read_text(encoding='utf-8'))
            stage = json.loads(next((Path(tmp) / 'steps').glob('*.json')).read_text(encoding='utf-8'))
            self.assertEqual(data['steps']['1']['name'], 'GrammarStep')
            entries = data['steps']['1']['entries_seconds']
            self.assertEqual(entries['total'], stage['duration_seconds'])
            self.assertGreaterEqual(entries['cache_key'], 0)
            self.assertEqual(stage['status'], 'failed')

    def test_profiling_config_is_boolean(self):
        self.assertFalse(resolve_config().profiling.enabled)
        self.assertTrue(resolve_config(overrides={'profiling': {'enabled': True}}).profiling.enabled)
        with self.assertRaises(ValueError):
            resolve_config(overrides={'profiling': {'enabled': 1}})

    def test_profiling_preserves_results_and_cache_hit_has_no_matching(self):
        with tempfile.TemporaryDirectory() as tmp:
            cache = StageCache(tmp, {})
            baseline = GrammarStep()
            cache.process(baseline, Artifact(fixture()))
            measured = GrammarStep(profile_rules=True)
            cache.refresh = {'all'}
            cache.process(measured, Artifact(fixture()))
            self.assertEqual(baseline.occurrences, measured.occurrences)
            self.assertEqual(baseline.spans, measured.spans)
            self.assertEqual(baseline.matches, measured.matches)
            self.assertIn('matching', measured._performance_entries)
            self.assertTrue(measured.performance['rules_seconds'])
            cache.refresh = set()
            hit = GrammarStep(profile_rules=True)
            cache.process(hit, Artifact(fixture()))
            self.assertEqual(hit.spans, baseline.spans)
            self.assertNotIn('matching', hit._performance_entries)
            self.assertIn('cache_read', hit._performance_entries)
            write_performance(tmp, {'1': {'entries_seconds': hit._performance_entries}})
            data = json.loads((Path(tmp) / 'performance.json').read_text(encoding='utf-8'))
            self.assertEqual(data['steps']['1']['entries_seconds'], hit._performance_entries)


if __name__ == '__main__':
    unittest.main()
