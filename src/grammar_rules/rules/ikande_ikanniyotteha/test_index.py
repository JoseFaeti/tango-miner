import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ikande_ikanniyotteha.index import RULE, match_ikande
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class IkandeTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '結果のいかんで決まる。': 'いかんで',
            '結果如何によっては中止する。': '如何によっては',
            '成績いかんによって合否が決まる。': 'いかんによって',
            '結果如何で決める。': '如何で',
            '遺憾です。': None,
            'いかんでしょ。': None,
            '如何でございますか。': None,
            '結果いかんを問わない。': None,
            '結果の いかんで決まる。': None,
            '結果いかんに よっては中止する。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ikande(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('結果のいかんによっては中止する。', [
            tok('結果', '結果', ('名詞',)), tok('の', 'の', ('助詞',)),
            tok('いかん', 'いかん', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('よっ', 'よる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('中止する。', '中止', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['いかん', 'に', 'よっ', 'て', 'は'])
        words = {'結果': object()}
        stage.add_cards(words)
        self.assertIn('結果', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'いかんによっては')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
