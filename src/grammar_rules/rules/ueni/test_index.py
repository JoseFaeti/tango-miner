import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.ueni.index import UENI_EXPRESSION, match_ueni


class UeniTests(unittest.TestCase):
    def test_sudachi_forms_and_spatial_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '高い上に、狭い。': '上に',
            '雨が降った上に、風も強い。': '上に',
            '便利な上に、安い。': '上に',
            '机の上に本がある。': None,
            '上に置く。': None,
            '高い上に。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ueni(aligned)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation_preserve_predicate(self):
        source = sentence('この部屋は高い上に、狭いです。', [
            tok('この部屋は', '部屋', ('名詞',)),
            tok('高い', '高い', ('形容詞', '一般', '*', '*', '*', '連体形-一般')),
            tok('上', '上', ('名詞', '普通名詞')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('、', '、', ('補助記号',)), tok('狭い', '狭い', ('形容詞',)),
            tok('です', 'です', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['上', 'に', 'です'])
        words = {'高い': object()}
        stage.add_cards(words)
        self.assertIn('高い', words)
        self.assertEqual(words[UENI_EXPRESSION].frequency, 2)
        self.assertEqual(words[UENI_EXPRESSION].sentences[0].surface_form, '上に')


if __name__ == '__main__':
    unittest.main()
