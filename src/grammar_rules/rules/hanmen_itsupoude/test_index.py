import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.hanmen_itsupoude.index import EXPRESSION, match_hanmen_itsupoude
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class HanmenItsupoudeTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '便利な反面、値段が高い。': ['反面'],
            '高い反面、便利だ。': ['反面'],
            '働く一方で、勉強する。': ['一方で'],
            '学生である一方で、働く。': ['一方で'],
            '反面教師だ。': [], '一方通行だ。': [],
            '一方で、雨が降った。': [], 'その反面、安い。': [],
            '増える一方だ。': [], '働く一方で。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_hanmen_itsupoude(aligned)], expected)

    def test_card_identity_and_lexical_evidence(self):
        source = sentence('働く一方で、勉強する。', [
            tok('働く', '働く', ('動詞', '一般', '*', '*', '*', '連体形-一般')),
            tok('一方', '一方', ('名詞', '普通名詞')),
            tok('で', 'で', ('助詞', '格助詞')),
            tok('、', '、', ('補助記号',)), tok('勉強する', '勉強する', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'働く': object(), '一方': object()}
        stage.add_cards(words)
        self.assertIn('働く', words)
        self.assertIn('一方', words)
        self.assertEqual(words[EXPRESSION].frequency, 2)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, '一方で')


if __name__ == '__main__':
    unittest.main()
