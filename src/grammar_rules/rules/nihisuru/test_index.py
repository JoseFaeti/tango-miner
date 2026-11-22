import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nihisuru.index import RULE, match_nihisuru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NihisuruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'去年に比する増加': 'に比する',
                 '兄に比して弱い': 'に比して',
                 '去年に比し増えた': 'に比し',
                 '兄に比した結果': 'に比した',
                 '兄と比する': None,
                 '兄に比します': None,
                 '兄に比される': None,
                 '兄に 比する': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nihisuru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('去年に比して今年は暑いですね。', [
            tok('去年', '去年', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('比し', '比する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('今年は暑いですね。', '今年', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '比し', 'て'])
        words = {'去年': object()}
        stage.add_cards(words)
        self.assertIn('去年', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に比して')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
