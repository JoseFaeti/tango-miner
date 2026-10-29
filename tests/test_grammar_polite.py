import unittest

from src.grammar_rules.helpers import polite_suffix_end, positive_verb_end, negative_predicate_end
from src.grammar_rules.rules.te_iku import match_te_iku


def tokens(parts):
    result = []
    offset = 0
    for surface, lemma, pos in parts:
        result.append({'surface': surface, 'lemma': lemma, 'pos': pos.split('/'),
                       'span': [offset, offset + len(surface)]})
        offset += len(surface)
    return result


class PoliteSuffixTests(unittest.TestCase):
    def test_complete_suffixes_and_allowed_forms(self):
        cases = [([('ます', 'ます', '助動詞')], 'positive'),
                 ([('まし', 'ます', '助動詞'), ('た', 'た', '助動詞')], 'positive'),
                 ([('ませ', 'ます', '助動詞'), ('ん', 'ぬ', '助動詞')], 'negative'),
                 ([('ませ', 'ます', '助動詞'), ('ん', 'ぬ', '助動詞'),
                   ('でし', 'です', '助動詞'), ('た', 'た', '助動詞')], 'negative'),
                 ([('ませんでした', 'ます', '助動詞')], 'negative'),
                 ([('ましょう', 'ます', '助動詞')], 'volitional'),
                 ([('ましょ', 'ます', '助動詞')], 'volitional')]
        for parts, form in cases:
            with self.subTest(parts=parts):
                ts = tokens(parts)
                self.assertEqual(polite_suffix_end(ts, 0, forms=(form,)), len(ts) - 1)
                self.assertIsNone(polite_suffix_end(ts, 0, forms=(('negative' if form == 'positive' else 'positive'),)))

    def test_incomplete_and_gapped_suffixes(self):
        for parts in [[('まし', 'ます', '助動詞')], [('ませ', 'ます', '助動詞')],
                      [('ませ', 'ます', '助動詞'), ('ん', 'ぬ', '助動詞'), ('でし', 'です', '助動詞')],
                      [('まし', 'ます', '助動詞'), ('た', 'た', '名詞')]]:
            self.assertIsNone(polite_suffix_end(tokens(parts), 0))
        ts = tokens([('まし', 'ます', '助動詞'), ('た', 'た', '助動詞')])
        ts[1]['span'][0] += 1
        self.assertIsNone(polite_suffix_end(ts, 0))

    def test_shared_verb_helpers_keep_form_constraints(self):
        base = [('あり', 'ある', '動詞')]
        neg = tokens(base + [('ませ', 'ます', '助動詞'), ('ん', 'ぬ', '助動詞')])
        args = dict(lemma='ある', base_forms={'ある'}, past_stems={'あっ'}, polite_stems={'あり'})
        self.assertIsNone(positive_verb_end(neg, 0, **args))
        self.assertEqual(negative_predicate_end(neg, 0, polite_lemma='ある', polite_stems={'あり'}), 2)
        self.assertIsNone(negative_predicate_end(neg[:-1], 0, polite_lemma='ある', polite_stems={'あり'}))

    def test_reservation_boundary_and_lexical_control(self):
        prefix = [('歩い', '歩く', '動詞/一般/*/*/*/連用形'), ('て', 'て', '助詞/接続助詞'),
                  ('いき', 'いく', '動詞/非自立可能')]
        suffix = [('ませ', 'ます', '助動詞'), ('ん', 'ぬ', '助動詞'),
                  ('でし', 'です', '助動詞'), ('た', 'た', '助動詞')]
        ts = tokens(prefix + suffix)
        self.assertEqual(list(match_te_iku(ts)), [(2, ts[-1]['span'][1])])
        self.assertEqual(list(match_te_iku(tokens(prefix + suffix[:-1]))), [])
        self.assertEqual(list(match_te_iku(tokens(prefix[2:] + suffix))), [])
        for surface in ('ましょう', 'ましょ'):
            ts = tokens(prefix + [(surface, 'ます', '助動詞'), ('か', 'か', '助詞')])
            self.assertEqual(list(match_te_iku(ts)), [(2, ts[-2]['span'][1])])


if __name__ == '__main__':
    unittest.main()
