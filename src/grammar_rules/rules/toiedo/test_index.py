import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiedo.index import RULE, match_toiedo
from src.grammar_rules.rules.toiedomo.index import RULE as LONG_RULE
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class ToiedoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼といえど例外ではない。': 'といえど',
            '子供と言えど許されない。': 'と言えど',
            '彼といえど。': 'といえど',
            '彼といえども例外ではない。': None,
            '彼と いえど違う。': None,
            'いえど違う。': None,
            '家どもが並ぶ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiedo(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_long_form(self):
        def source(long):
            ending = 'も' if long else ''
            tokens = [tok('子供', '子供', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
                      tok('言え', '言う', ('動詞',)), tok('ど', 'ど', ('助詞', '接続助詞'))]
            if long:
                tokens.append(tok('も', 'も', ('助詞', '係助詞')))
            tokens.append(tok('許されない。', '許す', ('動詞',)))
            return sentence('子供と言えど' + ending + '許されない。', tokens)

        stage = GrammarStep(rules=(RULE, LONG_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source(False), source(True)])))
        self.assertEqual([[t['surface'] for t in s.tokens if t.get('grammar_reserved')]
                          for s in result.data], [['と', '言え', 'ど'], ['と', '言え', 'ど', 'も']])
        words = {'子供': object()}
        stage.add_cards(words)
        self.assertIn('子供', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と言えど')
        self.assertEqual(words[LONG_RULE.expression].sentences[0].surface_form, 'と言えども')
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
