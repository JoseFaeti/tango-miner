import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.totomoni.index import RULE, match_totomoni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TotomoniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '友達とともに歩く。': 'とともに',
            '家族と共に暮らす。': 'と共に',
            '年を取るとともに変わる。': 'とともに',
            '便利であるとともに安全だ。': 'とともに',
            '美しいとともに便利だ。': 'とともに',
            '彼はともに歩いた。': None,
            '友達と ともに歩く。': None,
            '友達とともだちになる。': None,
            '彼とともに会う。': 'とともに',
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_totomoni(tokens)],
                                 [] if expected is None else [expected], raw)

    def test_reservation_and_identity(self):
        source = sentence('友達とともに公園を歩きました。', [
            tok('友達', '友達', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('とも', 'とも', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('公園を歩きました。', '歩く', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'とも', 'に'])
        words = {'友達': object()}
        stage.add_cards(words)
        self.assertIn('友達', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とともに')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
