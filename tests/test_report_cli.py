import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from src.ReportCLI import query


class ReportCLITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.old = root / '2026-01-01_1200_a'
        self.new = root / '2026-01-02_1200_b'
        for path, seconds in ((self.old, 10), (self.new, 15)):
            path.mkdir()
            summary = {'status': 'completed', 'duration_seconds': seconds, 'dependencies': {},
                       'anki': {'status': 'preview_only', 'plan': {'add': 2, 'modify': 0, 'delete': 0}},
                       'stages': [{'name': 'MergeTokensStep', 'duration_seconds': seconds},
                                  {'name': 'CompareOutputStep', 'comparison': {'status': 'compared', 'baseline': self.old.name, 'counts': {'added': 1}}}]}
            for filename, content in [('summary.json', summary), ('config.json', {}), ('input-manifest.json', [])]:
                (path / filename).write_text(json.dumps(content), encoding='utf-8')

    def test_performance_threshold_and_compatibility(self):
        data = query('performance', self.new, regressions=True, threshold=25)
        self.assertEqual(data['baseline'], self.old.name)
        self.assertEqual(data['timings'][0]['change_percent'], 50.0)
        self.assertTrue(data['compatibility']['inputs_identical'])
        self.assertEqual(query('performance', self.new, regressions=True, threshold=60)['timings'], [])

    def test_anki_plans_and_diff_are_distinguished(self):
        data = query('anki', self.new, changes_only=True)
        self.assertEqual(data['planned_changes'], {'add': 2})
        self.assertEqual(data['anki_status'], 'preview_only')
        self.assertEqual(query('diff', self.new)['comparison']['counts']['added'], 1)

    def test_missing_baseline_and_missing_metrics_are_explicit(self):
        data = query('warnings', self.old)
        self.assertIn('baseline_unavailable', [w['id'] for w in data['warnings']])
        self.assertEqual(query('summary', self.new)['candidates_at_definition'], {})

    def test_cli_json_and_error_exit(self):
        cmd = [sys.executable, 'tango_miner.py', 'report', 'performance', '--run', str(self.new), '--regressions', '--json']
        result = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['timings'][0]['stage'], 'MergeTokensStep')
        result = subprocess.run(cmd[:-3] + ['--run', str(self.new / 'missing')], capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
