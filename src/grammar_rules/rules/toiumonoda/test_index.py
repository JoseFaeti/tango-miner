import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.monoda.index import match_monoda
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiu.index import match_toiu
from src.grammar_rules.rules.toiumonoda.index import RULE, match_toiumonoda


class ToiumonodaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'それは無責任というものだ。': 'というものだ',
            'これこそ勇気というものです。': 'というものです',
            'それは無茶というものだった。': 'というものだった',
            'それは無茶というものでした。': 'というものでした',
            'それは無茶っていうものだ。': 'っていうものだ',
            '猫というものだ。': 'というものだ',
            '猫というものがいる。': None,
            'それは無茶というものではない。': None,
            'これは私のものだ。': None,
            'それは無茶というものだから困る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiumonoda(aligned)],
                                 [] if expected is None else [expected])
                if expected:
                    self.assertEqual(list(match_toiu(aligned)), [])
                    self.assertEqual(list(match_monoda(aligned)), [])

    def test_identity_and_card(self):
        tokens = [tok('それは', 'それ', ('名詞',)), tok('無茶', '無茶', ('名詞',)), tok('と', 'と', ('助詞',)),
                  tok('いう', 'いう', ('動詞',)), tok('もの', 'もの', ('名詞',)),
                  tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))]
        source = sentence('それは無茶というものだ。', tokens)
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertTrue(all(t.get('grammar_reserved') for t in reserved.data[0].tokens[2:6]))
        self.assertFalse(reserved.data[0].tokens[1].get('grammar_reserved'))
        words = {'もの': object()}
        stage.add_cards(words)
        self.assertIn(RULE, RULES)
        self.assertIn('もの', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'というものだ')


if __name__ == '__main__':
    unittest.main()
