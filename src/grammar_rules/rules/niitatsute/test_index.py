import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niitatsute.index import RULE, match_niitatsute
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiitatsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'この事態に至って初めて気づいた。': 'に至って',
                 '今にいたって後悔する。': None,
                 '駅に至って休んだ。': 'に至って',
                 '今に至っては遅い。': None, '結論に至る。': None,
                 '今に 至って気づいた。': None, '駅へ至って休んだ。': None,
                 '至って静かだ。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niitatsute(tokens)],
                                 [] if expected is None else [expected])

    def test_kana_when_analyzed_as_itaru(self):
        raw = [tok('今', '今', ('名詞',)), tok('に', 'に', ('助詞',)),
               tok('いたっ', 'いたる', ('動詞',)), tok('て', 'て', ('助詞',))]
        text = '今にいたって'
        tokens, error = aligned_tokens(text, raw)
        self.assertIsNone(error)
        self.assertEqual([text[a:b] for a, b in match_niitatsute(tokens)], ['にいたって'])

    def test_reservation_and_identity(self):
        source = sentence('事態に至って気づいた。', [
            tok('事態', '事態', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('至っ', '至る', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('気づいた。', '気づく', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '至っ', 'て'])
        words = {'事態': object()}
        stage.add_cards(words)
        self.assertIn('事態', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に至って')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
