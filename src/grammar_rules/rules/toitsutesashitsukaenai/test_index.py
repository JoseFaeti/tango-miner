import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toitsutesashitsukaenai.index import RULE, match_toitsutesashitsukaenai


class ToitsutesashitsukaenaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '天才と言って差し支えない。': 'と言って差し支えない',
            '天才といって差し支えありません。': 'といって差し支えありません',
            '天才と言って差し支えませんでした。': 'と言って差し支えませんでした',
            '天才と言って差し支えなかった。': 'と言って差し支えなかった',
            '天才と言って差し支えないです。': 'と言って差し支えないです',
            '天才と言って差しつかえない。': 'と言って差しつかえない',
            '天才と言っても差し支えない。': 'と言っても差し支えない',
            '差し支えない。': None,
            '天才と言って差し支える。': None,
            '天才と言って、差し支えない。': None,
            '天才と言って差し支えなかっ。': None,
            '天才と言って差し支えなければ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toitsutesashitsukaenai(aligned)],
                                 [] if expected is None else [expected])

    def test_identity_reservation_and_examples(self):
        tokens = [tok('天才', '天才', ('名詞',)), tok('と', 'と', ('助詞',)),
                  tok('言っ', '言う', ('動詞',)), tok('て', 'て', ('助詞',)),
                  tok('差し支え', '差し支える', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
                  tok('。', '。', ('補助記号',))]
        source = sentence('天才と言って差し支えない。', tokens)
        stage = GrammarStep()
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertTrue(all(t.get('grammar_reserved') for t in reserved.data[0].tokens[1:6]))
        self.assertFalse(reserved.data[0].tokens[0].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertIn(RULE, RULES)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と言って差し支えない')


if __name__ == '__main__':
    unittest.main()
