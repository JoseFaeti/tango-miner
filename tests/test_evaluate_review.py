import tempfile
import unittest
from pathlib import Path

from scripts.evaluate_review import evaluate, read_jsonl, sample_candidates, write_jsonl


class ReviewEvaluationTests(unittest.TestCase):
    def test_sampling_is_deterministic_stratified_and_excludes_missing_examples(self):
        rows = {f'{status}-{i}': {'status': status, 'frequency': 12 - i}
                for status in ('valid', 'dropped/review') for i in range(12)}
        examples = {word: {} for word in rows if not word.endswith('-11')}
        first = sample_candidates(rows, examples, 2, 'seed')
        self.assertEqual(first, sample_candidates(dict(reversed(list(rows.items()))), examples, 2, 'seed'))
        self.assertEqual(len(first), 12)
        self.assertEqual(len({stratum for _, _, stratum in first}), 6)
        self.assertTrue(all(word in examples for word, _, _ in first))

    def test_evaluation_counts_baseline_separately_and_does_not_score_unlabeled_rows(self):
        sample = {key: {'stratum': 'valid:high'} for key in ('a', 'b', 'c', 'd')}
        labels = {'a': {'baseline_correct': 'correct', 'gold_target': '猫', 'gold_entry_id': '1'},
                  'b': {'baseline_correct': 'incorrect', 'gold_target': '犬', 'gold_entry_id': '2'},
                  'c': {'baseline_correct': 'uncertain'},
                  'd': {'gold_target': '魚', 'gold_entry_id': '3'}}
        predictions = {'a': {'decision': 'select', 'target': '猫', 'entry_id': '1'},
                       'b': {'decision': 'select', 'target': '犬', 'entry_id': 'wrong'},
                       'c': {'decision': 'select', 'target': 'x', 'entry_id': 'x'}}
        counts = evaluate(sample, labels, predictions)['all']
        self.assertEqual((counts['sampled'], counts['scorable'], counts['correct'],
                          counts['incorrect'], counts['abstain_or_missing']), (4, 3, 1, 1, 1))
        self.assertEqual((counts['baseline_correct'], counts['baseline_incorrect'],
                          counts['baseline_uncertain']), (1, 1, 1))
        with self.assertRaises(ValueError):
            evaluate(sample, {'missing': {}}, {})

    def test_jsonl_rejects_duplicate_ids_and_does_not_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'sample.jsonl'
            write_jsonl(path, [{'id': 'a'}])
            with self.assertRaises(FileExistsError):
                write_jsonl(path, [{'id': 'b'}])
            self.assertEqual(read_jsonl(path), {'a': {'id': 'a'}})
            path.write_text('{"id":"a"}\n{"id":"a"}\n', encoding='utf-8')
            with self.assertRaises(ValueError):
                read_jsonl(path)


if __name__ == '__main__':
    unittest.main()
