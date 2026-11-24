import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niouji.index import RULE, match_niouji
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NioujiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '需要に応じ価格を調整する。': 'に応じ',
            '状況に応じ、変える。': 'に応じ',
            'それに応じ対応する。': 'に応じ',
            '状況におうじ変える。': 'におうじ',
            '呼びかけに応じ立ち上がる。': 'に応じ',
            '要求に応じる。': None,
            '状況に応じて変える。': None,
            '状況に応じまして変える。': None,
            '状況に応じた対応。': None,
            '状況に応じます。': None,
            '状況に応じない。': None,
            '状況に応じよう。': None,
            '状況に応じ。': None,
            '状況に応じ': None,
            '状況を応じ変える。': None,
            '状況に 応じ変える。': None,
            '応じ変える。': None,
            '状況に応えて変える。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niouji(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('人口の増加に応じ変わる。', [
            tok('人口', '人口', ('名詞',)), tok('の', 'の', ('助詞',)),
            tok('増加', '増加', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('応じ', '応ずる', ('動詞',)), tok('変わる。', '変わる', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '応じ'])
        words = {'増加': object()}
        stage.add_cards(words)
        self.assertIn('増加', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に応じ')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)

    def test_te_form_keeps_separate_identity(self):
        source = sentence('需要に応じて変える。', [
            tok('需要', '需要', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('応じ', '応ずる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('変える。', '変える', ('動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {}
        stage.add_cards(words)
        self.assertIn('〜に応じて', words)
        self.assertNotIn(RULE.expression, words)
