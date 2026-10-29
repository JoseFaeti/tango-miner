import unittest

from src.GrammarShadow import aligned_tokens
from src.grammar_rules.helpers import negative_suffix_end
from src.grammar_rules.rules.te_iru import match_te_iru
from src.grammar_rules.rules.te_iku import match_te_iku
from src.grammar_rules.rules.te_kuru import match_te_kuru
from src.grammar_rules.rules.tai import match_tai
from src.grammar_rules.rules.nda import match_nda


class CompleteNegativeTests(unittest.TestCase):
    def test_observed_sequences(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = [
            (match_te_iru, '覚えてなくってさ。', 'てなくって'),
            (match_te_iru, '読んでいなかったら困る。', 'でいなかったら'),
            (match_te_iru, 'やってなきゃいいけど。', 'てなきゃ'),
            (match_te_iku, '行っていかなかったら困る。', 'ていかなかったら'),
            (match_te_kuru, '行ってこなきゃ困る。', 'てこなきゃ'),
            (match_tai, '行きたくなかったら言って。', 'たくなかったら'),
            (match_nda, '行くんじゃなかったら言って。', 'んじゃなかったら'),
        ]
        for rule, text, expected in cases:
            with self.subTest(text=text):
                raw = [{'surface': m.surface(), 'lemma': m.dictionary_form(),
                        'pos': m.part_of_speech()}
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertIn(expected, [text[a:b] for a, b in rule(tokens)])

    def test_incomplete_forms_are_not_reserved(self):
        def token(surface, lemma, pos, start):
            return {'surface': surface, 'lemma': lemma, 'pos': [pos],
                    'span': [start, start + len(surface)]}
        for stem, ending in [('なくっ', 'て'), ('なけれ', 'ば')]:
            first = token(stem, 'ない', '助動詞', 0)
            second = token(ending, ending, '助詞', len(stem))
            with self.subTest(stem=stem):
                self.assertIsNone(negative_suffix_end([first], 0, pos='助動詞', extended=True))
                self.assertEqual(negative_suffix_end([first, second], 0, pos='助動詞', extended=True), 1)
                second['span'][0] += 1
                self.assertIsNone(negative_suffix_end([first, second], 0, pos='助動詞', extended=True))


if __name__ == '__main__':
    unittest.main()
