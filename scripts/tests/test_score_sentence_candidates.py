import unittest

from scripts.score_sentence_candidates import audit, score


def candidate(start, end, surface, target):
    return {'span': [start, end], 'surface': surface, 'target': target}


class SentenceScoreTests(unittest.TestCase):
    def setUp(self):
        self.sample = {'a': {'text': 'ないし', 'partition': 'development', 'group_id': 'g'}}
        self.labels = {'a': {'text': 'ないし', 'partition': 'development', 'reviewed': True,
                             'exhaustive': True, 'annotations': [
                                 {'alternatives': [candidate(0, 2, 'ない', 'ない'),
                                                   candidate(0, 3, 'ないし', 'ないし')]},
                                 {'alternatives': [candidate(2, 3, 'し', 'し')]}]}}

    def test_alternatives_and_overlapping_candidates(self):
        prediction = {'a': {'candidates': [candidate(0, 2, 'ない', 'ない'),
                                           candidate(2, 3, 'し', 'し'),
                                           candidate(0, 3, 'ないし', 'ないし')]}}
        result = score(self.sample, self.labels, prediction)['partitions']['development']
        self.assertEqual((result['gold_candidates'], result['predicted_candidates'],
                          result['matched_candidates']), (2, 3, 2))

    def test_one_proposal_cannot_match_two_items(self):
        self.labels['a']['annotations'].append(self.labels['a']['annotations'][0])
        result = score(self.sample, self.labels, {'a': {'candidates': [candidate(0, 2, 'ない', 'ない')]}})
        self.assertEqual(result['partitions']['development']['matched_candidates'], 1)

    def test_unreviewed_nonexhaustive_and_abstention(self):
        self.labels['a']['exhaustive'] = False
        self.assertEqual(score(self.sample, self.labels, {'a': {}})['partitions']['development']['non_exhaustive_lines'], 1)
        self.labels['a']['exhaustive'] = True
        self.assertEqual(score(self.sample, self.labels, {'a': {'abstain': True}})['partitions']['development']['abstained_lines'], 1)
        self.labels['a']['reviewed'] = False
        self.assertEqual(score(self.sample, self.labels, {'a': {}})['partitions']['development']['unreviewed_lines'], 1)

    def test_rejects_bad_spans_and_missing_rows(self):
        with self.assertRaises(ValueError):
            score(self.sample, self.labels, {'a': {'candidates': [candidate(0, 2, 'し', 'し')]}})
        with self.assertRaises(ValueError):
            score(self.sample, self.labels, {})

    def test_audit_detects_exact_leakage(self):
        sample = dict(self.sample, b={'text': 'ないし', 'partition': 'holdout', 'group_id': 'g'})
        self.assertEqual(audit(sample), {'groups_crossing_partitions': 1, 'exact_texts_crossing_partitions': 1})


if __name__ == '__main__':
    unittest.main()
