import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.shidaida.index import RULE, match_shidaida
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ShidaidaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '天気次第だ。': '次第だ', 'あなた次第です。': '次第です',
            '天気次第だった。': '次第だった', '天気次第でした。': '次第でした',
            '天気しだいだ。': 'しだいだ', '事の次第だ。': None,
            '準備ができ次第連絡する。': None, '天気次第で出発する。': None,
            '次第に暗くなる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_shidaida(aligned)],
                                 [] if expected is None else [expected])

    def test_card_retains_noun_and_reserves_marker(self):
        source = sentence('出発できるかは天気次第だ。', [
            tok('出発できるかは', '出発', ('名詞',)),
            tok('天気', '天気', ('名詞',)), tok('次第', '次第', ('名詞',)),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'天気': object()}
        stage.add_cards(words)
        self.assertIn('天気', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '次第だ')


if __name__ == '__main__':
    unittest.main()
