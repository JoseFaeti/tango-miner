import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikakete.index import RULE, match_nikakete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikaketeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '夜から朝にかけて雨が降る。': 'にかけて',
            '関東から東北に掛けて雪が降る。': 'に掛けて',
            '壁にかけて飾る。': 'にかけて',
            '時間をかけて作る。': None,
            '朝にかける。': None,
            '朝に かけて雨が降る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikakete(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('朝にかけて雨が降る。', [
            tok('朝', '朝', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('かけ', 'かける', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('雨が降る。', '降る', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'かけ', 'て'])
        words = {'朝': object()}
        stage.add_cards(words)
        self.assertIn('朝', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にかけて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
