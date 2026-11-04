import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ta_hou_ga_ii import RULE, match_ta_hou_ga_ii
from src.grammar_rules.rules.tests.test_grammar_te_iru import sentence, tok


class TaHouGaIiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '早く寝たほうがいい。': 'たほうがいい',
            '病院に行った方がいいです。': 'た方がいい',
            '酒を飲まないほうがいい。': 'ないほうがいい',
            '本を読んだ方が良い。': 'だ方が良い',
            '猫の方がいい。': None, '赤い方がいい。': None,
            '行く方がいい。': None, '行った方が悪い。': None,
            '行った 方がいい。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ta_hou_ga_ii(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        verb = ('動詞', '一般', '*', '*', '五段', '連用形-促音便')
        source = sentence('早く行った方がいい。', [
            tok('早く', '早い', ('形容詞',)), tok('行っ', '行く', verb),
            tok('た', 'た', ('助動詞',)), tok('方', '方', ('名詞',)),
            tok('が', 'が', ('助詞',)), tok('いい', '良い', ('形容詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['た', '方', 'が', 'いい'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertIn(RULE.expression, words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'た方がいい')


if __name__ == '__main__':
    unittest.main()
