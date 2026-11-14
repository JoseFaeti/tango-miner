import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.onozoite.index import RULE, match_onozoite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OnozoiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'日曜日を除いて毎日働く。': 'を除いて',
                 'それをのぞいて全部食べた。': 'をのぞいて',
                 'ほこりを除いて掃除する。': 'を除いて',
                 '窓を覗いて見た。': None,
                 '日曜日を除く。': None,
                 '日曜日を 除いて休む。': None,
                 '日曜日に除いて休む。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_onozoite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('日曜日を除いて毎日働いています。', [
            tok('日曜日', '日曜日', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('除い', '除く', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('毎日働いています。', '毎日', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '除い', 'て'])
        words = {'日曜日': object()}
        stage.add_cards(words)
        self.assertIn('日曜日', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を除いて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
