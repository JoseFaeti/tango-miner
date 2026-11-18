import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.katawara.index import EXPRESSION, match_katawara
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KatawaraTests(unittest.TestCase):
    def test_sudachi_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '会社で働くかたわら、小説を書いている。': ['かたわら'],
            '大学で教える傍ら、研究を続ける。': ['傍ら'],
            '母の傍らに座る。': [],
            '仕事のかたわら研究する。': [],
            '傍らに犬がいる。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_katawara(aligned)], expected)

    def test_boundaries_and_card_identity(self):
        verb = tok('働く', '働く', ('動詞', '一般', '*', '*', '五段', '連体形-一般'))
        word = tok('かたわら', '傍ら', ('名詞', '普通名詞'))
        source = sentence('働くかたわら研究する。', [verb, word, tok('研究する。', '研究', ('名詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'働く': object()}
        stage.add_cards(words)
        self.assertIn('働く', words)
        self.assertEqual(words[EXPRESSION].frequency, 2)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, 'かたわら')
        aligned, error = aligned_tokens('働く かたわら', [verb, word])
        self.assertIsNone(error)
        self.assertEqual(list(match_katawara(aligned)), [])


if __name__ == '__main__':
    unittest.main()
