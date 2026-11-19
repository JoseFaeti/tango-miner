import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nimashite.index import RULE, match_nimashite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NimashiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'以前に増して忙しい。': 'に増して',
                 '以前にまして忙しい。': 'にまして',
                 '去年にも増して忙しい。': None,
                 '水を増して煮る。': None,
                 '話しまして、帰った。': None,
                 '初めまして。': None,
                 '以前に 増して忙しい。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nimashite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('以前に増して忙しい。', [
            tok('以前', '以前', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('増し', '増す', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('忙しい。', '忙しい', ('形容詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '増し', 'て'])
        words = {'以前': object()}
        stage.add_cards(words)
        self.assertIn('以前', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に増して')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)

    def test_kana_overlap_preserves_both_analyses(self):
        source = sentence('以前にまして忙しい。', [
            tok('以前', '以前', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('まして', 'まして', ('副詞',)), tok('忙しい。', '忙しい', ('形容詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {}
        stage.add_cards(words)
        self.assertIn(RULE.expression, words)
        self.assertIn('〜まして', words)
