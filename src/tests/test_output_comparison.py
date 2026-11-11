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
            rows[0] = ['99', '猫', 'ねこ', '2.00', '2', 'new\nmeaning', 'tag', 'sentences: 1', "<span class='highlight'>例</span><br><span class='tag sentence-tag'>source</span>"]
            with (b / 'output.csv').open('w', encoding='utf-8', newline='') as f:
                csv.writer(f).writerows(rows)
            result = build_change_report(load_run(a), load_run(b))
            self.assertEqual(result['schema_version'], 4)
            self.assertEqual(set(result['changes']), {'added', 'removed', 'changed'})
            entries = {x['candidate']: x for group in result['changes'].values()
                       for x in (group if isinstance(group, list) else [group])}
            self.assertEqual([x['candidate'] for x in result['changes']['added']], sorted(['新', '犬']))
            self.assertEqual(result['changes']['removed']['candidate'], '旧')
            self.assertEqual(result['changes']['changed']['candidate'], '猫')
            self.assertEqual(result['counts'], {'added': 2, 'removed': 1, 'changed': 1})
            self.assertEqual(set(entries['猫']['fields']), {'score', 'reading', 'index', 'frequency', 'definition', 'tags', 'sentence_count', 'examples'})
            self.assertEqual(entries['猫']['fields']['definition'], ['definition', 'new\nmeaning'])
            self.assertEqual(entries['猫']['fields']['score'], [0.0, 99.0])
            self.assertIsInstance(entries['猫']['fields']['score'], list)
            self.assertNotIn('"before"', json.dumps(result))
            self.assertNotIn('"after"', json.dumps(result))
            self.assertEqual(entries['新']['sentence_count'], 0)
            self.assertEqual(entries['旧']['score'], 0)
            self.assertNotIn('word', entries['新'])
            self.assertEqual(entries['猫']['fields']['examples'], {'text': [None, "<span class='highlight'>例</span>"]})

    def test_example_changes_only_include_changed_properties(self):
        from src.RunComparison import example_changes, report_examples
        before = [{'text': 'one'}, {'text': 'two'}]
        after = [{'text': 'one'}, {'text': 'changed'}]
        self.assertEqual(example_changes(before, after), [
            {'text': ['one']},
            {'text': ['two', 'changed']},
        ])
        self.assertEqual(example_changes(before[:1], []), [
            {'text': ['one', None]},
        ])
        self.assertEqual(example_changes(before, before[:1]), [
            {'text': ['one']},
            {'text': ['two', None]},
        ])
        self.assertEqual(example_changes([], after[:1]), [
            {'text': [None, 'one']},
        ])
        self.assertEqual(report_examples("<span class='highlight'>猫</span> &amp; 犬<br><span class='tag sentence-tag'>source</span>"),
                         [{'text': "<span class='highlight'>猫</span> & 犬"}])

    def test_scored_examples_are_rejected(self):
        from src.RunComparison import report_examples
        with self.assertRaisesRegex(ValueError, 'Scored example'):
            report_examples("猫<br><span class='tag sentence-tag'>source - 2.5</span>")

    def test_single_values_are_unwrapped_recursively(self):
        from src.RunComparison import unwrap_single_values
        self.assertEqual(unwrap_single_values({'examples': [
            {'text': ['unchanged']},
        ], 'empty': [], 'removed': [None, 'old']}),
            {'examples': {'text': 'unchanged'},
             'empty': [], 'removed': [None, 'old']})

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

    def test_automatic_scored_baseline_is_skipped_and_next_run_compares(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = make_run(root, '2026-10-27_1200_a', [('猫', 1)])
            with (old / 'output.csv').open('r', encoding='utf-8', newline='') as f:
                rows = list(csv.reader(f))
            rows[0][7:] = ['sentences: 1', "猫<br><span class='tag sentence-tag'>FF7 - 918.14</span>"]
            with (old / 'output.csv').open('w', encoding='utf-8', newline='') as f:
                csv.writer(f).writerows(rows)
            current = make_run(root, '2026-10-27_1201_b', [('猫', 1)], status='running')
            step = CompareOutputStep(current / 'output.csv')
            step.process(Artifact({}))
            report = json.loads(step.path.read_text(encoding='utf-8'))
            self.assertEqual(report['status'], 'no_baseline')
            self.assertEqual(report['baseline_skipped'], {'run': old.name, 'reason': 'scored_examples'})
            with self.assertRaisesRegex(ValueError, 'Scored example'):
                CompareOutputStep(current / 'output.csv', old).process(Artifact({}))
            (current / 'summary.json').write_text('{"status":"completed"}', encoding='utf-8')
            next_run = make_run(root, '2026-10-27_1202_c', [('猫', 1)], status='running')
            step = CompareOutputStep(next_run / 'output.csv')
            step.process(Artifact({}))
            self.assertEqual(step.comparison['status'], 'compared')
            self.assertEqual(step.comparison['baseline'], current.name)

    def test_scored_current_output_still_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            make_run(root, '2026-10-27_1200_a', [('猫', 1)])
            current = make_run(root, '2026-10-27_1201_b', [('猫', 1)], status='running')
            with (current / 'output.csv').open('r', encoding='utf-8', newline='') as f:
                rows = list(csv.reader(f))
            rows[0][7:] = ['sentences: 1', "猫<br><span class='tag sentence-tag'>FF7 - 918.14</span>"]
            with (current / 'output.csv').open('w', encoding='utf-8', newline='') as f:
                csv.writer(f).writerows(rows)
            with self.assertRaisesRegex(ValueError, 'Scored example'):
                CompareOutputStep(current / 'output.csv').process(Artifact({}))

    def test_explicit_baseline_overrides_latest(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = make_run(root, '2026-10-27_1200_a', [('猫', 1)])
            make_run(root, '2026-10-27_1201_b', [('犬', 1)])
            current = make_run(root, '2026-10-27_1202_c', [('猫', 1)], status='running')
            step = CompareOutputStep(current / 'output.csv', old)
            step.process(Artifact({}))
            self.assertEqual(step.comparison['baseline'], old.name)
            self.assertEqual(step.comparison['counts'], {'added': 0, 'removed': 0, 'changed': 0})


if __name__ == '__main__':
    unittest.main()
