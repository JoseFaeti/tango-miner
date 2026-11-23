import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niataru.index import RULE, match_niataru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NiataruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'これは違反に当たる。': 'に当たる', 'それは罪にあたる。': 'にあたる',
            '昨日は祝日に当たった。': 'に当たった', '今日は祝日に当たります。': 'に当たります',
            'その日は祝日に当たりました。': 'に当たりました', '違反に当たれば罰する。': 'に当たれば',
            '球が壁に当たる。': 'に当たる', '違反に当たらない。': None,
            '開始に当たって挨拶する。': None, '壁には当たる。': None,
            '壁に 当たる。': None, '壁に当たっ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niataru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('これは違反に当たる。', [
            tok('これは', 'これ', ('代名詞',)), tok('違反', '違反', ('名詞',)),
            tok('に', 'に', ('助詞',)), tok('当たる', '当たる', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '当たる'])
        words = {'違反': object()}
        stage.add_cards(words)
        self.assertIn('違反', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に当たる')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
