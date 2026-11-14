import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tetamaranai.index import RULE, match_tetamaranai


class TetamaranaiTests(unittest.TestCase):
    def test_observed_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '会いたくてたまらない。': 'てたまらない',
            '嬉しくてたまらなかった。': 'てたまらなかった',
            '心配でたまらない。': 'でたまらない',
            '欲しくて堪らない。': 'て堪らない',
            '見たくてたまりません。': 'てたまりません',
            '忙しくてたまらなくて困る。': 'てたまらなくて',
            'お金が貯まらない。': None,
            '雨水が流れて貯まらない。': None,
            '嬉しくてたまる。': None,
            '嬉しくて たまらない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tetamaranai(aligned)],
                                 [] if expected is None else [expected])

    def test_incomplete_and_card_integration(self):
        adjective = tok('嬉しく', '嬉しい', ('形容詞', '一般', '*', '*', '形容詞', '連用形-一般'))
        te = tok('て', 'て', ('助詞', '接続助詞'))
        verb = tok('たまら', 'たまる', ('動詞',))
        incomplete = [adjective, te, verb, tok('なかっ', 'ない', ('助動詞',))]
        aligned, error = aligned_tokens('嬉しくてたまらなかっ', incomplete)
        self.assertIsNone(error)
        self.assertEqual(list(match_tetamaranai(aligned)), [])
        source = sentence('嬉しくてたまらない。', [adjective, te, verb,
                          tok('ない', 'ない', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['て', 'たまら', 'ない'])
        words = {'嬉しい': object()}
        stage.add_cards(words)
        self.assertIn('嬉しい', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'てたまらない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)


if __name__ == '__main__':
    unittest.main()
