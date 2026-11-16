import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikakete.index import RULE as NIKAKETE
from src.grammar_rules.rules.nikaketeha.index import RULE, match_nikaketeha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikaketehaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '料理にかけては彼が一番だ。': 'にかけては',
            '腕前に掛けては誰にも負けない。': 'に掛けては',
            '朝にかけては雨が降る。': 'にかけては',
            '壁にかけては外す。': 'にかけては',
            '朝にかけて雨が降る。': None,
            '時間をかけては作れない。': None,
            '料理にかけて は負けない。': None,
            '料理に かけては負けない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikaketeha(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_overlap(self):
        source = sentence('料理にかけては彼が一番だ。', [
            tok('料理', '料理', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('かけ', 'かける', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('は', 'は', ('助詞', '係助詞')), tok('彼が一番だ。', '彼', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'かけ', 'て', 'は'])
        words = {'料理': object()}
        stage.add_cards(words)
        self.assertIn('料理', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にかけては')
        self.assertIn(NIKAKETE.expression, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
