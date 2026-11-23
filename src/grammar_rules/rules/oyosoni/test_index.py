import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.oyosoni.index import RULE, match_oyosoni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OYosoniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        from sudachipy import dictionary, tokenizer
        analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        cases = {
            '心配をよそに出かけた。': 'をよそに',
            '反対を余所に計画を進めた。': 'を余所に',
            '予想をよそに勝った。': 'をよそに',
            '家をよそに建てた。': 'をよそに',
            'よそに行く。': None,
            '心配をよそへ置いた。': None,
            '心配を よそに出かけた。': None,
            '心配によそに行く。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_oyosoni(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_attached_word(self):
        source = sentence('心配をよそに出かけた。', [
            tok('心配', '心配', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('よそ', 'よそ', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('出かけた。', '出かける', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'よそ', 'に'])
        words = {'心配': object()}
        stage.add_cards(words)
        self.assertIn('心配', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をよそに')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
