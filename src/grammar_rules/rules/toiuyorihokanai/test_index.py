import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.hokanai_shikanai.index import RULE as HOKANAI
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiuyorihokanai.index import RULE
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class ToiuyorihokanaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '奇跡と言うよりほかない。': 'と言うよりほかない',
            '残念というより他はなかった。': 'というより他はなかった',
            '奇跡と言うよりほかありません。': 'と言うよりほかありません',
            '奇跡というよりほかはありませんでした。': 'というよりほかはありませんでした',
            '奇跡と言うより美しい。': None,
            '彼が言うより他はない。': None,
            '奇跡と言うよりほかにもある。': None,
            '奇跡と言うより ほかない。': None,
            '奇跡と言うよりほか なかった。': None,
            '行くほかない。': None,
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
        source = sentence('奇跡と言うよりほかない。', [
            tok('奇跡', '奇跡', ('名詞', '普通名詞', '一般')),
            tok('と', 'と', ('助詞', '格助詞')),
            tok('言う', '言う', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('より', 'より', ('助詞', '格助詞')),
            tok('ほか', 'ほか', ('名詞', '普通名詞', '副詞可能')),
            tok('ない', 'ない', ('形容詞',)), tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        stage = GrammarStep(rules=(RULE, HOKANAI))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        words = {'奇跡': object()}
        stage.add_cards(words)
        self.assertIn('奇跡', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と言うよりほかない')
        self.assertNotIn(HOKANAI.expression, words)
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '言う', 'より', 'ほか', 'ない'])
