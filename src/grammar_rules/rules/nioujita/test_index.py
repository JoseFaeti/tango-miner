import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nioujita.index import RULE, match_nioujita
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NioujitaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '能力に応じた仕事を選ぶ。': 'に応じた',
            '状況に応じた対応。': 'に応じた',
            'それに応じた方法。': 'に応じた',
            '状況におうじた対応。': 'におうじた',
            '呼びかけに応じた人。': 'に応じた',
            '要求に応じた。': None,
            '状況に応じた': None,
            '状況に応じて変える。': None,
            '状況に応じ変える。': None,
            '要求に応じる人。': None,
            '要求に応じなかった人。': None,
            '要求に応じました。': None,
            '状況を応じた対応。': None,
            '応じた対応。': None,
            '状況に 応じた対応。': None,
            '状況に応じ た対応。': None,
            '状況に応じた 対応。': None,
            '状況に応じた、対応。': None,
            '期待に応えた人。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nioujita(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('需要に応じた価格を設定します。', [
            tok('需要', '需要', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('応じ', '応ずる', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('価格', '価格', ('名詞',)), tok('を設定します。', '設定', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '応じ', 'た'])
        words = {'需要': object()}
        stage.add_cards(words)
        self.assertIn('需要', words)
        self.assertEqual([t['surface'] for t in data.data[0].tokens
                          if not t.get('grammar_reserved')], ['需要', '価格', 'を設定します。'])
        self.assertNotIn('〜に応じ', words)
        self.assertNotIn('〜に応じて', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に応じた')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
