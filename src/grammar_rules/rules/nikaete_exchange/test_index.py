import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikaete_exchange.index import RULE, match_nikaete_exchange
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikaeteExchangeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'現金を外貨に替えて旅行する。': 'に替えて',
                 '新しい服に替えて出かける。': 'に替えて',
                 '挨拶に代えて手紙を送る。': None,
                 '父に代わって出席する。': None,
                 '服を替えて出かける。': None,
                 '外貨に替える。': None,
                 '外貨に 替えて旅行する。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikaete_exchange(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('現金を外貨に替えて旅行する。', [
            tok('現金', '現金', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('外貨', '外貨', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('替え', '替える', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('旅行する。', '旅行', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '替え', 'て'])
        words = {'外貨': object()}
        stage.add_cards(words)
        self.assertIn('外貨', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に替えて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
