import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitsuite.index import RULE, match_nitsuite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NitsuiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'日本について話す。': 'について', '問題についての本。': 'について',
                 'これについては話す。': 'について', '問題につきまして話す。': 'につきまして',
                 '机に付いている。': None, '本をついて読む。': None,
                 '日本に ついて話す。': None, '机についている。': 'について'}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitsuite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('日本について詳しく話しましょう。', [
            tok('日本', '日本', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('つい', 'つく', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('詳しく話しましょう。', '詳しい', ('形容詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'つい', 'て'])
        words = {'日本': object()}
        stage.add_cards(words)
        self.assertIn('日本', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'について')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
