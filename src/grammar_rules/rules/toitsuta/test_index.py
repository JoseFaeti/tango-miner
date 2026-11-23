import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toitsuta.index import RULE, match_toitsuta
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class ToitsutaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'りんごやみかんといった果物が好きだ。': 'といった',
            '犬と言った動物を飼う。': 'と言った',
            '百人といったところだ。': None,
            'さすが勇者の血を継ぐものといったところかしら。': None,
            '特殊な合金といった方が近いでしょうか。': None,
            '特殊な合金と言ったほうが近い。': None,
            '百人といったところに行く。': None,
            '犬などといった動物が好きだ。': 'といった',
            '東京や大阪といった都市を訪ねる。': 'といった',
            '彼は行くと言った。': None,
            '友達と行った店だ。': None,
            '彼は行くといったら帰った。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toitsuta(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_identity_and_reservation(self):
        tokens = [tok('犬', '犬', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
                  tok('いっ', 'いう', ('動詞',)), tok('た', 'た', ('助動詞',)),
                  tok('動物', '動物', ('名詞',)), tok('が', 'が', ('助詞',)),
                  tok('好き', '好き', ('形状詞',)), tok('だ', 'だ', ('助動詞',)),
                  tok('。', '。', ('補助記号',))]
        source = sentence('犬といった動物が好きだ。', tokens)
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertFalse(data.data[0].tokens[0].get('grammar_reserved'))
        self.assertTrue(all(t.get('grammar_reserved') for t in data.data[0].tokens[1:4]))
        self.assertFalse(data.data[0].tokens[4].get('grammar_reserved'))
        words = {'犬': object(), '動物': object()}
        stage.add_cards(words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'といった')
        self.assertIn('犬', words)
        self.assertIn('動物', words)


if __name__ == '__main__':
    unittest.main()
