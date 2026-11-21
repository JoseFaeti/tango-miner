import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niiwasereba.index import RULE, match_niiwasereba
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiiwaserebaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'彼に言わせれば簡単だ。': 'に言わせれば',
                 '先生にいわせれば違う。': 'にいわせれば',
                 '彼に言わせればいい。': 'に言わせれば',
                 '彼に言わせると違う。': None, '彼に言えばよい。': None,
                 '彼に話させればよい。': None, '彼に 言わせればよい。': None,
                 'に言わせればよい。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niiwasereba(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼に言わせれば簡単だ。', [
            tok('彼', '彼', ('代名詞',)), tok('に', 'に', ('助詞',)),
            tok('言わ', '言う', ('動詞',)), tok('せれ', 'せる', ('助動詞',)),
            tok('ば', 'ば', ('助詞',)), tok('簡単', '簡単', ('形状詞',)),
            tok('だ。', 'だ', ('助動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '言わ', 'せれ', 'ば'])
        words = {'彼': object()}
        stage.add_cards(words)
        self.assertIn('彼', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に言わせれば')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
