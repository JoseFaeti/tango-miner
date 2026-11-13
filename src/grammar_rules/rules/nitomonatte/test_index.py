import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitomonatte.index import RULE, match_nitomonatte
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NitomonatteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '人口の増加に伴って住宅が増える。': 'に伴って',
            '年を取るに伴って変わる。': 'に伴って',
            '変化にともなって増える。': 'にともなって',
            '痛みに伴って熱が出る。': 'に伴って',
            '変化に伴い増える。': None,
            '伴って増える。': None,
            '変化に 伴って増える。': None,
            '変化を伴って増える。': None,
            '変化に伴った問題。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitomonatte(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('人口の増加に伴って変わる。', [
            tok('人口', '人口', ('名詞',)), tok('の', 'の', ('助詞',)),
            tok('増加', '増加', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('伴っ', '伴う', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('変わる。', '変わる', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '伴っ', 'て'])
        words = {'増加': object()}
        stage.add_cards(words)
        self.assertIn('増加', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に伴って')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
