import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nimotozuite.index import RULE, match_nimotozuite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NimotozuiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'事実に基づいて判断する。': 'に基づいて',
                 '証拠に基づいて作る。': 'に基づいて',
                 'それに基づいて判断する。': 'に基づいて',
                 '規則にもとづいて作る。': 'にもとづいて',
                 '事実に基づく。': None, '事実に基づいた計画。': None,
                 '基づいて作る。': None, '規則に 基づいて作る。': None,
                 '頭に気づいて': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nimotozuite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('事実に基づいて判断する。', [
            tok('事実', '事実', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('基づい', '基づく', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('判断する。', '判断する', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '基づい', 'て'])
        words = {'事実': object()}
        stage.add_cards(words)
        self.assertIn('事実', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に基づいて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
