import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.toiedomo.index import RULE, match_toiedomo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToiedomoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼といえども例外ではない。': 'といえども',
            '子供と言えども許されない。': 'と言えども',
            '彼といえども。': 'といえども',
            '彼といえど例外ではない。': None,
            '彼と いえども違う。': None,
            'いえども違う。': None,
            '家どもが並ぶ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiedomo(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('子供と言えども許されない。', [
            tok('子供', '子供', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('言え', '言う', ('動詞', '一般', '*', '*', '五段-ワア行', '仮定形-一般')),
            tok('ど', 'ど', ('助詞', '接続助詞')), tok('も', 'も', ('助詞', '係助詞')),
            tok('許されない。', '許す', ('動詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '言え', 'ど', 'も'])
        words = {'子供': object()}
        stage.add_cards(words)
        self.assertIn('子供', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と言えども')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
