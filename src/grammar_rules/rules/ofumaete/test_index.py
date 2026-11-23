import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ofumaete.index import RULE, match_ofumaete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OfumaeteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'結果を踏まえて判断する。': 'を踏まえて',
                 'それを踏まえて考える。': 'を踏まえて',
                 '経験をふまえて決める。': 'をふまえて',
                 '結果を踏まえた計画。': None, '結果を踏まえる。': None,
                 '踏まえて考える。': None, '結果を 踏まえて考える。': None,
                 '足を踏んで歩く。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ofumaete(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('結果を踏まえて判断する。', [
            tok('結果', '結果', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('踏まえ', '踏まえる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('判断する。', '判断する', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '踏まえ', 'て'])
        words = {'結果': object()}
        stage.add_cards(words)
        self.assertIn('結果', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を踏まえて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
