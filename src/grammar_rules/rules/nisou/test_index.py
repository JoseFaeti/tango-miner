import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nisotsute.index import RULE as TE_RULE
from src.grammar_rules.rules.nisou.index import RULE, match_nisou
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NisouTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'方針に沿う。': 'に沿う', '方針に沿う計画。': 'に沿う',
                 '川に沿った道。': 'に沿った', '方針に沿った。': 'に沿った',
                 'それに沿う。': 'に沿う', '道にそう。': None,
                 '方針に沿って進む。': None, '沿う道。': None,
                 '道に 沿う。': None, '頭に剃った。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nisou(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_separate_connective(self):
        source = sentence('方針に沿った計画を進める。', [
            tok('方針', '方針', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('沿っ', '沿う', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('計画を進める。', '計画', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '沿っ', 'た'])
        words = {'方針': object()}
        stage.add_cards(words)
        self.assertIn('方針', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に沿った')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
        self.assertNotIn(TE_RULE.expression, words)
