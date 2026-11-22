import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.omedoni.index import RULE, match_omedoni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OmedoniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'来年をめどに完成する。': 'をめどに',
                 '三月を目処に終える。': 'を目処に',
                 '年度末を目途に進める。': 'を目途に',
                 '来年をめどにして進める。': 'をめどに',
                 '完成のめどになる。': None, 'めどにする。': None,
                 '来年をめどとする。': None, '来年を めどにする。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_omedoni(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_lexical_candidate(self):
        source = sentence('来年をめどに完成する。', [
            tok('来年', '来年', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('めど', 'めど', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('完成する。', '完成する', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'めど', 'に'])
        words = {'来年': object()}
        stage.add_cards(words)
        self.assertIn('来年', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をめどに')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
