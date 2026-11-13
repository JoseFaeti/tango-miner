import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.itsupouda.index import RULE, match_itsupouda
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ItsupoudaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '物価が上がる一方だ。': '一方だ', '人口が減る一方です。': '一方です',
            '悪化する一方だった。': '一方だった', '増える一方でした。': '一方でした',
            '働く一方で、勉強する。': None, '一方だ。': None,
            '一方通行だ。': None, '増えた一方だ。': None,
            '高い一方だ。': None, '増える一方の問題。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_itsupouda(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card_evidence(self):
        verb = tok('増える', '増える', ('動詞', '一般', '*', '*', '*', '終止形-一般'))
        noun = tok('一方', '一方', ('名詞', '普通名詞'))
        copula = tok('だ', 'だ', ('助動詞',))
        tokens, error = aligned_tokens('増える 一方だ', [verb, noun, copula])
        self.assertIsNone(error)
        self.assertEqual(list(match_itsupouda(tokens)), [])
        source = sentence('人口が増える一方だ。', [tok('人口が', '人口', ('名詞',)), verb, noun, copula, tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        self.assertIn(('itsupouda', 6, 9), stage.spans[0])
        words = {'増える': object(), '一方': object()}
        stage.add_cards(words)
        self.assertIn('増える', words)
        self.assertIn('一方', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '一方だ')


if __name__ == '__main__':
    unittest.main()
