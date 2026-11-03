import unittest

from scripts.next_grammar import next_point


class NextGrammarTests(unittest.TestCase):
    def test_first_unchecked_point_skips_other_states(self):
        queue = ('- [~] [partial](points/partial.md)\n'
                 '- [x] [done](points/done.md)\n'
                 '- [ ] [〜ために](points/tame-ni.md)\n'
                 '- [ ] [later](points/later.md)\n')
        self.assertEqual(next_point(queue), {
            'name': '〜ために', 'path': 'docs/grammar/points/tame-ni.md',
        })

    def test_no_unchecked_point(self):
        self.assertIsNone(next_point('- [~] [partial](points/partial.md)\n'))
