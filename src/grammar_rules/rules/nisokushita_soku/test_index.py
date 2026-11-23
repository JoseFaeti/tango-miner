import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nisokushita_soku.index import RULE, match_nisokushita
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NisokushitaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'実情に即した計画。': 'に即した',
                 '現実に即した対応。': 'に即した',
                 'それに即した方法。': 'に即した',
                 '規則にそくした計画。': 'にそくした',
                 '実情に即して行う。': None, '実情に即する。': None,
                 '規則に則した計画。': None, '即した計画。': None,
                 '実情に 即した計画。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nisokushita(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('実情に即した計画を立てる。', [
            tok('実情', '実情', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('即し', '即する', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('計画を立てる。', '計画', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '即し', 'た'])
        words = {'実情': object(), '計画': object()}
        stage.add_cards(words)
        self.assertIn('実情', words)
        self.assertIn('計画', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に即した')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
