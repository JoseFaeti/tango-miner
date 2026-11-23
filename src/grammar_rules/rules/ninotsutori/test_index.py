import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ninotsutori.index import RULE, match_ninotsutori
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NinotsutoriTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'規則に則り行動する。': 'に則り',
                 '法律に則り、判断する。': 'に則り',
                 'それに則り行う。': 'に則り',
                 '規則にのっとり行う。': 'にのっとり',
                 '規則に則ります。': None, '規則に則る。': None,
                 '規則に則って行う。': None, '則り行う。': None,
                 '規則に 則り行う。': None, '船に乗っ取り行く。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ninotsutori(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('規則に則り行動する。', [
            tok('規則', '規則', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('則り', '則る', ('動詞',)), tok('行動する。', '行動する', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '則り'])
        words = {'規則': object()}
        stage.add_cards(words)
        self.assertIn('規則', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に則り')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
