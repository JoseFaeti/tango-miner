import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.todoujini.index import RULE, match_todoujini
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class TodoujiniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '卒業と同時に就職した。': 'と同時に',
            'ドアが開くと同時に走った。': 'と同時に',
            '彼は優しいと同時に厳しい。': 'と同時に',
            '彼と同時に出発した。': 'と同時に',
            '同時に出発した。': None,
            '彼と同時刻に出発した。': None,
            '卒業と同時は無理だ。': None,
            '卒業と 同時に就職した。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_todoujini(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('卒業と同時に就職した。', [
            tok('卒業', '卒業', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('同時', '同時', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('就職した。', '就職', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '同時', 'に'])
        words = {'卒業': object()}
        stage.add_cards(words)
        self.assertIn('卒業', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と同時に')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
