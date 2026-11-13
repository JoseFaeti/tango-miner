import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ochiyuushinni.index import RULE, match_ochiyuushinni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OchiyuushinniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '東京を中心に活動する。': 'を中心に',
            '若者を中心に人気がある。': 'を中心に',
            '東京を中心にした地域。': 'を中心に',
            '東京を中心とした地域。': None,
            '町の中心に住む。': None,
            '東京を 中心に活動する。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ochiyuushinni(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('東京を中心に活動する。', [
            tok('東京', '東京', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('中心', '中心', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('活動する。', '活動', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '中心', 'に'])
        words = {'東京': object()}
        stage.add_cards(words)
        self.assertIn('東京', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を中心に')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
