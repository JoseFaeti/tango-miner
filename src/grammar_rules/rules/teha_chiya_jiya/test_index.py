import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.teha_chiya_jiya.index import RULE, match_teha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TehaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '食べては寝る。': 'ては', '読んでは忘れる。': 'では',
            '食べちゃだめ。': 'ちゃ', '読んじゃ困る。': 'じゃ',
            '学生じゃない。': None, 'お茶を飲む。': None,
            '食べちゃった。': None, '食べている。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_teha(tokens)],
                                 [] if expected is None else [expected])

    def test_split_boundary_and_reservation(self):
        verb = tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '連用形-一般'))
        parts = [tok('彼は毎日ご飯を', 'ご飯', ('名詞',)), verb, tok('て', 'て', ('助詞', '接続助詞')),
                 tok('は', 'は', ('助詞', '係助詞')), tok('寝る生活を続けています。', '寝る', ('動詞',))]
        source = sentence('彼は毎日ご飯を食べては寝る生活を続けています。', parts)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual(stage.occurrences[RULE.id].examples[0].surface_form, 'ては')
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['て', 'は'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertIn(RULE.expression, words)


if __name__ == '__main__':
    unittest.main()
