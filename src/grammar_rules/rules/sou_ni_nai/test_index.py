import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nai.index import RULE as NAI
from src.grammar_rules.rules.sou_ni_nai.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class SouNiNaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降りそうにない。': 'そうにない',
            '仕事が終わりそうになかった。': 'そうになかった',
            '彼は来そうになくて心配だ。': 'そうになくて',
            '雨が降りそうになければ行く。': 'そうになければ',
            '彼は来そうにないです。': 'そうにない',
            '雨が降りそうだ。': None,
            '雨が降るそうにない。': None,
            'そうにない。': None,
            '元気そうにない。': None,
            '雨が降りそうに ない。': None,
            '雨が降りそうになかっ。': None,
            '雨が降りそうになる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('雨が降りそうにない。', [
            tok('雨が', '雨', ('名詞',)),
            tok('降り', '降る', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('そう', 'そう', ('形状詞', '助動詞語幹')),
            tok('に', 'だ', ('助動詞',)), tok('ない', 'ない', ('形容詞',)),
            tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        stage = GrammarStep(rules=(RULE, NAI))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['そう', 'に', 'ない'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {'降る': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'そうにない')
        self.assertIn('降る', words)


if __name__ == '__main__':
    unittest.main()
