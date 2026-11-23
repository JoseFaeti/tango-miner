import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.teshikarubekida.index import RULE, match_teshikarubekida
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class TeshikarubekidaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '罰せられてしかるべきだ。': 'てしかるべきだ',
            '褒められて然るべきです。': 'て然るべきです',
            '驚いてしかるべきだった。': 'てしかるべきだった',
            '読んでしかるべきでした。': 'でしかるべきでした',
            '認められてしかるべきこと。': 'てしかるべき',
            'しかるべきだ。': None, '叱るべきだ。': None,
            '学生でしかるべきだ。': None, '食べて しかるべきだ。': None,
            '食べてしかるべきだっ': None, '食べてしかる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_teshikarubekida(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_overlap(self):
        source = sentence('彼は罰せられてしかるべきだ。', [
            tok('彼は', '彼', ('名詞',)),
            tok('罰せ', '罰する', ('動詞', '一般', '*', '*', 'サ行変格', '未然形-セ')),
            tok('られ', 'られる', ('助動詞', '*', '*', '*', '助動詞-レル', '連用形-一般')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('しかる', 'しかり', ('動詞', '一般', '*', '*', '文語ラ行変格', '連体形-一般')),
            tok('べき', 'べし', ('助動詞',)),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        reserved = [t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')]
        self.assertEqual(reserved[-4:], ['て', 'しかる', 'べき', 'だ'])
        words = {'罰する': object()}
        stage.add_cards(words)
        self.assertIn('罰する', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'てしかるべきだ')
        self.assertIn('〜べきだ / 〜べきではない', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)


if __name__ == '__main__':
    unittest.main()
