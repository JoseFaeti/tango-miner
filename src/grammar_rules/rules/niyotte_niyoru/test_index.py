import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niyotte_niyoru.index import RULE, match_niyotte_niyoru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiyotteNiyoruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'人によって違う。': 'によって', '事故による影響。': 'による',
                 '人によっては違う。': 'によって', 'これによって変わる。': 'によって',
                 '駅に寄って帰る。': None, '人によると違う。': None,
                 '人によれば違う。': None, '人に よって違う。': None,
                 '彼によった。': None, '人に依って変わる。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niyotte_niyoru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('事故による影響は大きかった。', [
            tok('事故', '事故', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('よる', 'よる', ('動詞',)), tok('影響', '影響', ('名詞',)),
            tok('は大きかった。', '大きい', ('形容詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'よる'])
        words = {'事故': object(), '影響': object()}
        stage.add_cards(words)
        self.assertIn('事故', words)
        self.assertIn('影響', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'による')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
