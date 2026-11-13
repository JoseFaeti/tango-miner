import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nioujite.index import RULE, match_nioujite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NioujiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'需要に応じて変える。': 'に応じて',
                 'それに応じて決める。': 'に応じて',
                 '状況におうじて変える。': 'におうじて',
                 '状況に応じまして変える。': 'に応じまして',
                 '声に応じて答える。': 'に応じて',
                 '要求に応じる。': None, '状況に応じた対応。': None,
                 '応じて答える。': None, '状況に 応じて変える。': None,
                 '状況に応えて変える。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nioujite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('需要に応じて価格を調整します。', [
            tok('需要', '需要', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('応じ', '応ずる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('価格を調整します。', '価格', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '応じ', 'て'])
        words = {'需要': object()}
        stage.add_cards(words)
        self.assertIn('需要', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に応じて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
