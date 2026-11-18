import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.shidaideha.index import RULE, match_shidaideha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ShidaidehaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '天気次第では中止する。': '次第では',
            'あなた次第では変わる。': '次第では',
            '天気しだいでは中止する。': 'しだいでは',
            '天気次第で出発する。': None,
            '事の次第では驚いた。': None,
            '次第に暗くなる。': None,
            '準備ができ次第連絡する。': None,
            '結果次第では中止する。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_shidaideha(aligned)],
                                 [] if expected is None else [expected])

    def test_card_retains_noun_and_reserves_marker(self):
        source = sentence('天気次第では中止します。', [
            tok('天気', '天気', ('名詞',)), tok('次第', '次第', ('名詞',)),
            tok('で', 'で', ('助詞',)), tok('は', 'は', ('助詞',)),
            tok('中止します。', '中止', ('名詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'天気': object()}
        stage.add_cards(words)
        self.assertIn('天気', words)
        self.assertNotIn('〜次第で', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '次第では')


if __name__ == '__main__':
    unittest.main()
