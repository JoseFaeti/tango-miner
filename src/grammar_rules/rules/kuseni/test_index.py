import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kuseni.index import RULE, match_kuseni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KuseniTests(unittest.TestCase):
    def test_requires_adjacent_tokens(self):
        text = '学生の くせに'
        raw = [tok('学生', '学生', ('名詞', '普通名詞')),
               tok('の', 'の', ('助詞', '格助詞')),
               tok('くせ', 'くせ', ('名詞', '普通名詞')),
               tok('に', 'に', ('助詞', '格助詞'))]
        aligned, error = aligned_tokens(text, raw)
        self.assertIsNone(error)
        self.assertEqual(list(match_kuseni(aligned)), [])

    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '知っているくせに言わない。': 'くせに',
            '学生のくせに偉そうだ。': 'くせに',
            '静かなくせに怖い。': 'くせに',
            '高いくせに壊れた。': 'くせに',
            '知らないくせに。': 'くせに',
            '知ってるクセに。': 'クセに',
            '食べるクセになりそう。': None,
            '食べるクセになっちまって。': None,
            '悪いくせになっている。': None,
            '知っているくせに泣いた。': 'くせに',
            '彼の癖に困る。': None,
            '悪い癖に悩む。': None,
            'くせに言う。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kuseni(aligned)],
                                 [expected] if expected else [])

    def test_card_and_reservation(self):
        source = sentence('学生のくせに偉そうだ。', [
            tok('学生', '学生', ('名詞', '普通名詞')),
            tok('の', 'の', ('助詞', '格助詞')),
            tok('くせ', 'くせ', ('名詞', '普通名詞')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('偉そうだ。', '偉い', ('形容詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['くせ', 'に'])
        words = {'学生': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'くせに')
        self.assertIn('学生', words)

    def test_habit_tokens_are_not_reserved(self):
        source = sentence('食べるクセになりそう。', [
            tok('食べる', '食べる', ('動詞', '一般', '*', '*', '下一段', '連体形-一般')),
            tok('クセ', 'クセ', ('名詞', '普通名詞')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('なり', 'なる', ('動詞', '非自立可能')),
            tok('そう。', 'そう', ('形状詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertFalse(any(t.get('grammar_reserved') for t in data.data[0].tokens))
        words = {'クセ': object()}
        stage.add_cards(words)
        self.assertIn('クセ', words)
        self.assertNotIn(RULE.expression, words)
