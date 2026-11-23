import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.niatatsute_nisaishite.index import RULE as SHORT_RULE
from src.grammar_rules.rules.nisaishiteha.index import RULE, match_nisaishiteha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NisaishitehaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '出発に際しては注意する。': 'に際しては',
            '契約するに際しては確認する。': 'に際しては',
            '開会に際して挨拶する。': None,
            '開会に際しての挨拶。': None,
            '開会に際して は挨拶する。': None,
            '開会を際しては挨拶する。': None,
            '出発に当たっては注意する。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nisaishiteha(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_overlap(self):
        source = sentence('開会に際しては挨拶する。', [
            tok('開会', '開会', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('際し', '際する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('挨拶する。', '挨拶', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '際し', 'て', 'は'])
        words = {'開会': object()}
        stage.add_cards(words)
        self.assertIn('開会', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に際しては')
        self.assertEqual(words[SHORT_RULE.expression].sentences[0].surface_form, 'に際して')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
