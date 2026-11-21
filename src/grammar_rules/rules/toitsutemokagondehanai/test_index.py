import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toitsutemokagondehanai.index import RULE, match_toitsutemokagondehanai


class ToitsutemokagondehanaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '日本一と言っても過言ではない。': 'と言っても過言ではない',
            '天才といっても過言ではありません。': 'といっても過言ではありません',
            '天才と言っても過言じゃない。': 'と言っても過言じゃない',
            '天才と言っても過言ではなかった。': 'と言っても過言ではなかった',
            '天才と言っても過言ではありませんでした。': 'と言っても過言ではありませんでした',
            '天才と言っても過言ではないです。': 'と言っても過言ではないです',
            '天才と言っても過言だ。': None,
            '天才と言っても冗談ではない。': None,
            '過言ではない。': None,
            '天才と言っても、過言ではない。': None,
            '天才と言っても過言ではなかっ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toitsutemokagondehanai(aligned)],
                                 [] if expected is None else [expected])

    def test_identity_reservation_and_overlap(self):
        tokens = [tok('天才', '天才', ('名詞',)), tok('と', 'と', ('助詞',)),
                  tok('言っ', '言う', ('動詞',)), tok('て', 'て', ('助詞',)),
                  tok('も', 'も', ('助詞',)), tok('過言', '過言', ('名詞',)),
                  tok('で', 'で', ('助詞',)), tok('は', 'は', ('助詞',)),
                  tok('ない', 'ない', ('形容詞',)), tok('。', '。', ('補助記号',))]
        source = sentence('天才と言っても過言ではない。', tokens)
        stage = GrammarStep()
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertTrue(all(t.get('grammar_reserved') for t in reserved.data[0].tokens[1:9]))
        self.assertFalse(reserved.data[0].tokens[0].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertIn(RULE, RULES)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と言っても過言ではない')
        self.assertNotIn('じゃない / ではない', words)


if __name__ == '__main__':
    unittest.main()
