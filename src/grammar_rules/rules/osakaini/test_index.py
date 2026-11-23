import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.osakaini.index import RULE, match_osakaini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OsakainiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'卒業を境に生活が変わった。': 'を境に',
                 'その日を境にして会わなくなった。': 'を境に',
                 '川を境に町が分かれる。': 'を境に',
                 'それを境に変わった。': 'を境に',
                 '境になる。': None, '境を越える。': None,
                 '卒業を境として変わった。': None, '卒業を 境に変わった。': None,
                 '卒業を環境にした。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_osakaini(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_lexical_candidate(self):
        source = sentence('卒業を境に変わった。', [
            tok('卒業', '卒業', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('境', '境', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('変わった。', '変わる', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '境', 'に'])
        words = {'卒業': object()}
        stage.add_cards(words)
        self.assertIn('卒業', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を境に')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
