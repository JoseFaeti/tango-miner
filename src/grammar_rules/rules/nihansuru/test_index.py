import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nihansuru.index import RULE, match_nihansuru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NihansuruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'規則に反する行動。': 'に反する',
                 'これに反する。': 'に反する',
                 '法律に反します。': 'に反します',
                 '予想に反して成功した。': None,
                 '規則と反する行動。': None,
                 '規則に 反する行動。': None,
                 '反する意見。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nihansuru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('規則に反する行動は許されない。', [
            tok('規則', '規則', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('反する', '反する', ('動詞',)), tok('行動は許されない。', '行動', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '反する'])
        words = {'規則': object()}
        stage.add_cards(words)
        self.assertIn('規則', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に反する')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
