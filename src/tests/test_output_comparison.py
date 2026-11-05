import csv
import json
import tempfile
import unittest
from pathlib import Path

from src.Artifact import Artifact
from src.RunComparison import build_change_report, load_run
from src.steps.CompareOutputStep.index import CompareOutputStep
from scripts.tests.test_compare_runs import make_run


class OutputComparisonTests(unittest.TestCase):
    def test_all_fields_transitions_additions_and_removals(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            a = make_run(root, '2026-10-27_1200_a', [('猫', 1), ('旧', 1)], [('犬', 1)])
            b = make_run(root, '2026-10-27_1201_b', [('猫', 2), ('犬', 1), ('新', 1)])
            with (b / 'output.csv').open('r', encoding='utf-8', newline='') as f:
                rows = list(csv.reader(f))
            rows[0] = ['99', '猫', 'ねこ', '2.00', '2', 'new\nmeaning', 'tag', 'sentences: 1', "<span class='highlight'>例</span><br><span class='tag sentence-tag'>source - 2.5</span>"]
            with (b / 'output.csv').open('w', encoding='utf-8', newline='') as f:
                csv.writer(f).writerows(rows)
            result = build_change_report(load_run(a), load_run(b))
            self.assertEqual(result['schema_version'], 3)
            self.assertEqual(set(result['changes']), {'added', 'removed', 'changed'})
            entries = {x['candidate']: x for group in result['changes'].values() for x in group}
            self.assertEqual([x['candidate'] for x in result['changes']['added']], sorted(['新', '犬']))
            self.assertEqual([x['candidate'] for x in result['changes']['removed']], ['旧'])
            self.assertEqual([x['candidate'] for x in result['changes']['changed']], ['猫'])
            self.assertEqual(result['counts'], {'added': 2, 'removed': 1, 'changed': 1})
            self.assertEqual(set(entries['猫']['fields']), {'score', 'reading', 'index', 'frequency', 'definition', 'tags', 'sentence_count', 'examples'})
            self.assertEqual(entries['猫']['fields']['definition'], ['definition', 'new\nmeaning'])
            self.assertEqual(entries['猫']['fields']['score'], [0.0, 99.0])
            self.assertTrue(all(isinstance(value, list) for value in entries['猫']['fields'].values()))
            self.assertNotIn('"before"', json.dumps(result))
            self.assertNotIn('"after"', json.dumps(result))
            self.assertEqual(entries['新']['sentence_count'], 0)
            self.assertEqual(entries['旧']['score'], 0)
            self.assertNotIn('word', entries['新'])
            self.assertEqual(entries['猫']['fields']['examples'][0], {'text': [None, "<span class='highlight'>例</span>"], 'score': [None, 2.5]})

    def test_example_changes_only_include_changed_properties(self):
        from src.RunComparison import example_changes, report_examples
        before = [{'text': 'one', 'score': 1.0}, {'text': 'two', 'score': 2.0}]
        after = [{'text': 'one', 'score': 3.0}, {'text': 'changed', 'score': 2.0}]
        self.assertEqual(example_changes(before, after), [
            {'text': ['one'], 'score': [1.0, 3.0]},
            {'text': ['two', 'changed'], 'score': [2.0]},
        ])
        self.assertEqual(example_changes(before[:1], []), [
            {'text': ['one', None], 'score': [1.0, None]},
        ])
        self.assertEqual(example_changes(before, before[:1]), [
            {'text': ['one'], 'score': [1.0]},
            {'text': ['two', None], 'score': [2.0, None]},
        ])
        self.assertEqual(example_changes([], after[:1]), [
            {'text': [None, 'one'], 'score': [None, 3.0]},
        ])
        self.assertEqual(report_examples("<span class='highlight'>猫</span> &amp; 犬<br><span class='tag sentence-tag'>source - -1.5</span>"),
                         [{'text': "<span class='highlight'>猫</span> & 犬", 'score': -1.5}])

    def test_step_first_run_and_previous_completed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = make_run(root, '2026-10-27_1200_a', [('猫', 1)], status='running')
            step = CompareOutputStep(first / 'output.csv')
            artifact = Artifact({})
            self.assertIs(step.process(artifact), artifact)
            first_report = json.loads(step.path.read_text(encoding='utf-8'))
            self.assertEqual(first_report['status'], 'no_baseline')
            self.assertEqual(first_report['changes'], {'added': [], 'removed': [], 'changed': []})
            summary = first / 'summary.json'
            summary.write_text('{"status":"completed"}', encoding='utf-8')
            make_run(root, '2026-10-27_1201_b', [], status='failed')
            current = make_run(root, '2026-10-27_1202_c', [('犬', 2)], status='running')
            step = CompareOutputStep(current / 'output.csv')
            step.process(artifact)
            self.assertEqual(step.comparison['baseline'], first.name)
            self.assertEqual(step.comparison['counts']['added'], 1)

    def test_malformed_latest_baseline_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            make_run(root, '2026-10-27_1200_a', [])
            bad = make_run(root, '2026-10-27_1201_b', [('猫', 1)], [('猫', 1)])
            current = make_run(root, '2026-10-27_1202_c', [], status='running')
            with self.assertRaisesRegex(ValueError, 'Duplicate candidate'):
                CompareOutputStep(current / 'output.csv').process(Artifact({}))
            self.assertFalse((current / 'csv-changes.json').exists())


if __name__ == '__main__':
    unittest.main()
