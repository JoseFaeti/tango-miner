import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitomonau.index import RULE, match_nitomonau
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NitomonauTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '人口の増加に伴う問題。': 'に伴う',
            '年を取るに伴う変化。': 'に伴う',
            '変化にともなう問題。': 'にともなう',
            '痛みに伴う症状。': 'に伴う',
            'それに伴う問題。': 'に伴う',
            '変化に伴います。': None,
            '変化に伴って増える。': None,
            '変化に伴い増える。': None,
            '変化に伴った問題。': None,
            '変化に伴わない問題。': None,
            '危険を伴う仕事。': None,
            '変化に伴う。': None,
            '変化に伴うと考える。': None,
            '変化に 伴う問題。': None,
            '変化 に伴う問題。': None,
            '変化に伴う 問題。': None,
            '伴う問題。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitomonau(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('人口の増加に伴う問題。', [
            tok('人口', '人口', ('名詞',)), tok('の', 'の', ('助詞',)),
            tok('増加', '増加', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('伴う', '伴う', ('動詞',)), tok('問題', '問題', ('名詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '伴う'])
        words = {'増加': object(), '問題': object()}
        stage.add_cards(words)
        self.assertIn('増加', words)
        self.assertIn('問題', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に伴う')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
