import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikaete.index import RULE, match_nikaete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikaeteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'挨拶に代えて手紙を送る。': 'に代えて',
                 '現金に代えてカードで払う。': 'に代えて',
                 '命に代えても守る。': 'に代えて',
                 '手紙に替えて送る。': None,
                 '父に代わって出席する。': None,
                 '手紙を代えて送る。': None,
                 '手紙に代える。': None,
                 '手紙に 代えて送る。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikaete(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('挨拶に代えて手紙を送る。', [
            tok('挨拶', '挨拶', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('代え', '代える', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('手紙を送る。', '手紙', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '代え', 'て'])
        words = {'挨拶': object()}
        stage.add_cards(words)
        self.assertIn('挨拶', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に代えて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
