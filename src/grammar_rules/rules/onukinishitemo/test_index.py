import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.onukinishite.index import RULE as SHORT_RULE
from src.grammar_rules.rules.onukinishitemo.index import RULE, match_onukinishitemo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OnukinishitemoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '冗談を抜きにしても話す。': ['を抜きにしても'],
            '彼をぬきにしても考える。': ['をぬきにしても'],
            '彼を抜きにしても語れない。': ['を抜きにしても'],
            '歯を抜きにしても病院へ行く。': ['を抜きにしても'],
            '彼を抜きにして話す。': [],
            '彼を抜きにしては語れない。': [],
            '彼を抜きにして も考える。': [],
            '彼を 抜きにしても考える。': [],
            '彼を抜きにしたも話す。': [],
            '冗談抜きにしても話す。': [],
            '彼を抜いても考える。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_onukinishitemo(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('冗談を抜きにしても話す。', [
            tok('冗談', '冗談', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('抜き', '抜き', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('も', 'も', ('助詞',)), tok('話す。', '話す', ('動詞',)),
        ])
        stage = GrammarStep(rules=(SHORT_RULE, RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '抜き', 'に', 'し', 'て', 'も'])
        words = {'冗談': object()}
        stage.add_cards(words)
        self.assertIn('冗談', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を抜きにしても')
        self.assertEqual(words[SHORT_RULE.expression].sentences[0].surface_form, 'を抜きにして')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
