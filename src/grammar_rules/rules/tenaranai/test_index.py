import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tenaranai.index import RULE, match_tenaranai


class TenaranaiTests(unittest.TestCase):
    def test_observed_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '会いたくてならない。': 'てならない',
            '嬉しくてならなかった。': 'てならなかった',
            '心配でならない。': 'でならない',
            '欲しくて成らない。': 'て成らない',
            '見たくてなりません。': 'てなりません',
            '忙しくてならなくて困る。': 'てならなくて',
            'お金が足りない。': None,
            '静かにならない。': None,
            '嬉しくてなる。': None,
            '嬉しくて ならない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tenaranai(aligned)],
                                 [] if expected is None else [expected])

    def test_incomplete_and_card_integration(self):
        adjective = tok('嬉しく', '嬉しい', ('形容詞', '一般', '*', '*', '形容詞', '連用形-一般'))
        te = tok('て', 'て', ('助詞', '接続助詞'))
        verb = tok('なら', 'なる', ('動詞',))
        incomplete = [adjective, te, verb, tok('なかっ', 'ない', ('助動詞',))]
        aligned, error = aligned_tokens('嬉しくてならなかっ', incomplete)
        self.assertIsNone(error)
        self.assertEqual(list(match_tenaranai(aligned)), [])
        source = sentence('嬉しくてならない気持ち。', [adjective, te, verb,
                          tok('ない', 'ない', ('助動詞',)), tok('気持ち', '気持ち', ('名詞',)),
                          tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['て', 'なら', 'ない'])
        words = {'嬉しい': object()}
        stage.add_cards(words)
        self.assertIn('嬉しい', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'てならない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)


if __name__ == '__main__':
    unittest.main()
