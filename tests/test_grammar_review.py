import json
from pathlib import Path
import tempfile
import unittest

from src.GrammarReview import main, sample


def row(number, construction, evidence):
    identity = str(number)
    return {'sentence_id': 's' + identity, 'source': 'source', 'tag': None,
            'normalized_sentence': 'んだ', 'tokens': [], 'rule_version': 'v1',
            'proposals': [{'id': identity, 'construction_id': construction,
                           'evidence': evidence, 'span': [0, 2], 'surface': 'んだ',
                           'component_token_ids': []}]}


class GrammarReviewTests(unittest.TestCase):
    def test_bounded_stratified_sample_and_population(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'shadow.jsonl'
            rows = [row(i, 'nda', ['inside']) for i in range(20)]
            rows += [row(i + 20, 'nda', ['split']) for i in range(4)]
            path.write_text(''.join(json.dumps(r) + '\n' for r in rows), encoding='utf-8')
            selected, totals = sample(path, 3, 'seed')
            again, _ = sample(path, 3, 'seed')
            self.assertEqual(selected, again)
            self.assertEqual(len(selected), 6)
            self.assertEqual(totals[('nda', ('inside',))], 20)
            self.assertEqual({r['stratum_population'] for r in selected}, {4, 20})
            self.assertTrue(all(not r['reviewed'] and r['judgment'] is None for r in selected))

    def test_never_overwrites_review_file_and_rejects_zero_limit(self):
        with tempfile.TemporaryDirectory() as temp:
            path, output = Path(temp) / 'in.jsonl', Path(temp) / 'out.jsonl'
            path.write_text(json.dumps(row(1, 'nda', [])) + '\n', encoding='utf-8')
            output.write_text('existing review', encoding='utf-8')
            self.assertEqual(main(['--input', str(path), '--output', str(output)]), 1)
            self.assertEqual(output.read_text(encoding='utf-8'), 'existing review')
            with self.assertRaises(ValueError):
                sample(path, 0, 'seed')


if __name__ == '__main__':
    unittest.main()
