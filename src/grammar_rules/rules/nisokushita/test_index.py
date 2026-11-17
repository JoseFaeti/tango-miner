import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nisokushita.index import RULE, match_nisokushita
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NisokushitaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'規則に則した計画。': 'に則した',
                 '法律に則した対応。': 'に則した',
                 'それに則した方法。': 'に則した',
                 '規則にそくした計画。': 'にそくした',
                 '規則に則して行う。': None, '規則に則する。': None,
                 '規則に即した計画。': None, '則した計画。': None,
                 '規則に 則した計画。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nisokushita(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('規則に則した計画を立てる。', [
            tok('規則', '規則', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('則し', '則する', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('計画を立てる。', '計画', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '則し', 'た'])
        words = {'規則': object(), '計画': object()}
        stage.add_cards(words)
        self.assertIn('規則', words)
        self.assertIn('計画', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に則した')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
