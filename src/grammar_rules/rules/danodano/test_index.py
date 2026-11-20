import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.danodano.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DanodanoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '猫だの犬だのがいる。': ['だの'],
            '猫だの、犬だの、鳥だのがいる。': ['だの', 'だの'],
            '高いだの安いだのと文句を言う。': ['だの'],
            '猫だのがいる。': [], '猫だの。犬だのがいる。': [],
            '猫だ の犬だのがいる。': [], '猫なのだ。犬なのだ。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(tokens)], expected,
                                 [(t['surface'], t['pos']) for t in tokens])

    def test_reservation_and_identity(self):
        text = '猫だの犬だのがいる。'
        raw = [tok('猫', '猫', ('名詞',)), tok('だ', 'だ', ('助動詞',)),
               tok('の', 'の', ('助詞',)), tok('犬', '犬', ('名詞',)),
               tok('だ', 'だ', ('助動詞',)), tok('の', 'の', ('助詞',)),
               tok('が', 'が', ('助詞',)), tok('いる', 'いる', ('動詞',)),
               tok('。', '。', ('補助記号',))]
        stage = GrammarStep(rules=(RULE,))
        artifact = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([sentence(text, raw)])))
        self.assertFalse(artifact.data[0].tokens[3].get('grammar_reserved'))
        words = {'犬': object()}
        stage.add_cards(words)
        self.assertIn('犬', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'だの')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
