import hashlib
import tempfile
import unittest
from pathlib import Path

from scripts.compare_japanese_analyzers import align, choose_groups, collect_raw_lines, compare, focus_report


class AnalyzerComparisonTests(unittest.TestCase):
    def test_groups_do_not_cross_partitions(self):
        manifest = [{'path': f'{i} [group{i // 2}].txt'} for i in range(10)]
        groups = choose_groups(manifest, 'seed', 5)
        self.assertEqual(len(groups), 5)
        self.assertEqual(sum(partition == 'holdout' for _, _, partition in groups), 1)
        self.assertTrue(all(len(files) == 2 for _, files, _ in groups))
        reverse = choose_groups(list(reversed(manifest)), 'seed', 5)
        self.assertEqual([(g, p) for g, _, p in groups], [(g, p) for g, _, p in reverse])

    def test_sampling_keeps_original_lines_and_checks_manifest_hash(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'script [Game].txt'
            raw = '猫！\n  日本語  \nNo Japanese\n'.encode('utf8')
            path.write_bytes(raw)
            run = {'manifest': [{'path': str(path), 'sha256': hashlib.sha256(raw).hexdigest()}],
                   'config': {'input': {'encoding': 'utf-8'}}}
            rows = collect_raw_lines(run, 'seed', 1, 5)
            self.assertEqual({row['text'] for row in rows}, {'猫！', '  日本語  '})
            self.assertEqual(rows, collect_raw_lines(run, 'seed', 1, 5))
            path.write_bytes(raw + b'changed')
            with self.assertRaises(ValueError):
                collect_raw_lines(run, 'seed', 1, 5)

    def test_alignment_keeps_source_offsets_and_refuses_guesses(self):
        text = '猫 … 犬'
        tokens = [{'surface': '猫'}, {'surface': ''}, {'surface': '…'}, {'surface': '犬'}]
        self.assertEqual([t['span'] for t in align(text, tokens)], [[0, 1], [2, 3], [4, 5]])
        with self.assertRaises(ValueError):
            align('猫と犬', [{'surface': '猫'}, {'surface': '犬'}])

    def test_compare_counts_disagreement_not_accuracy_and_reports_errors(self):
        sample = {'a': {'partition': 'holdout'}, 'b': {'partition': 'holdout'}}
        left = {'a': {'tokens': [{'span': [0, 2]}], 'error': None},
                'b': {'tokens': [], 'error': 'failed'}}
        right = {'a': {'tokens': [{'span': [0, 1]}, {'span': [1, 2]}], 'error': None},
                 'b': {'tokens': [], 'error': None}}
        count = compare(sample, left, right)['holdout']
        self.assertEqual((count['sentences'], count['different_span_sets'], count['analyzer_errors']), (2, 1, 1))
        with self.assertRaises(ValueError):
            compare(sample, {}, right)

    def test_focus_report_does_not_treat_an_exact_token_as_a_correct_meaning(self):
        sample = {'a': {'text': 'ない', 'focus': 'ない', 'focus_span': [0, 2], 'expected_interpretation': 'fragment'},
                  'b': {'text': 'ない', 'focus': 'ない', 'focus_span': [0, 2], 'expected_interpretation': 'supported'}}
        rows = {'a': {'tokens': [{'span': [0, 2]}], 'error': None},
                'b': {'tokens': [{'span': [0, 1]}, {'span': [1, 2]}], 'error': None}}
        self.assertEqual(focus_report(sample, rows), {'unsupported_exact': 1, 'supported_not_exact': 1})


if __name__ == '__main__':
    unittest.main()
