import unittest

from src.grammar_rules.alignment import aligned_tokens


class AlignmentTests(unittest.TestCase):
    def test_aligns_repeated_tokens_and_preserves_source_offsets(self):
        tokens = [{'surface': '語', 'lemma': '語', 'pos': ('名詞',)},
                  {'surface': '語', 'lemma': '語', 'pos': ('名詞',)}]
        aligned, error = aligned_tokens('語 語', tokens)
        self.assertIsNone(error)
        self.assertEqual([token['span'] for token in aligned], [[0, 1], [2, 3]])
        self.assertEqual(aligned[0]['pos'], ['名詞'])

    def test_rejects_missing_source_text(self):
        aligned, error = aligned_tokens('読んでいる', [{'surface': '読ん'}, {'surface': 'いる'}])
        self.assertIsNone(aligned)
        self.assertEqual(error, 'cannot align token 1')

    def test_rejects_unconsumed_source_text(self):
        aligned, error = aligned_tokens('語尾', [{'surface': '語'}])
        self.assertIsNone(aligned)
        self.assertEqual(error, 'token stream omits source text')
