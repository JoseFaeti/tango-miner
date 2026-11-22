import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nihanshite.index import RULE, match_nihanshite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NihanshiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'予想に反して成功した。': 'に反して',
                 '期待に反して雨が降った。': 'に反して',
                 'これに反してはならない。': 'に反して',
                 '方針に反しまして行動した。': 'に反しまして',
                 '規則に反する行動。': None, '規則と反して行動する。': None,
                 '予想に 反して成功した。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nihanshite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('予想に反して成功した。', [
            tok('予想', '予想', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('反し', '反する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('成功した。', '成功', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '反し', 'て'])
        words = {'予想': object()}
        stage.add_cards(words)
        self.assertIn('予想', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に反して')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
