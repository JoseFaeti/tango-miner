import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.onukinishite.index import RULE as SHORT_RULE
from src.grammar_rules.rules.onukinishiteha.index import RULE, match_onukinishiteha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OnukinishitehaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼を抜きにしては語れない。': ['を抜きにしては'],
            '冗談をぬきにしては話せない。': ['をぬきにしては'],
            '歯を抜きにしては病院へ行けない。': ['を抜きにしては'],
            '彼を抜きにして語れない。': [],
            '彼を抜きにして は語れない。': [],
            '彼を 抜きにしては語れない。': [],
            '彼を抜きにした会議。': [],
            'なくしては成功しない。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_onukinishiteha(tokens)], expected)

    def test_overlap_reservation_identity_and_registration(self):
        source = sentence('冗談を抜きにしては語れない。', [
            tok('冗談', '冗談', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('抜き', '抜き', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('語れない。', '語れる', ('動詞',)),
        ])
        stage = GrammarStep(rules=(SHORT_RULE, RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '抜き', 'に', 'し', 'て', 'は'])
        words = {'冗談': object()}
        stage.add_cards(words)
        self.assertIn('冗談', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を抜きにしては')
        self.assertEqual(words[SHORT_RULE.expression].sentences[0].surface_form, 'を抜きにして')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
