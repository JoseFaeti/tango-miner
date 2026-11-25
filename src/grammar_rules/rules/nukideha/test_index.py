import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nukide_nukinishite.index import RULE as SHORT_RULE
from src.grammar_rules.rules.nukideha.index import RULE, match_nukideha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NukidehaTests(unittest.TestCase):
    def test_sudachi_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(
                config_path='sudachi.config.json', dict='full'
            ).create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '部長抜きでは決められない。': ['抜きでは'],
            '冗談ぬきでは話せない。': ['ぬきでは'],
            '砂糖抜きでは食べられない。': ['抜きでは'],
            '彼抜きでは成立しない。': ['抜きでは'],
            '彼抜きで考える。': [],
            '彼抜きにしては成立しない。': [],
            '彼を抜きでは語れない。': [],
            '彼 抜きでは決められない。': [],
            '彼抜き では決められない。': [],
            '彼抜きで は決められない。': [],
            '手抜きでは困る。': [],
            '追い抜きでは勝てない。': [],
            'ら抜きでは困る。': [],
            '抜きでは困る。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nukideha(tokens)], expected)

    def test_overlap_reservation_identity_and_registration(self):
        source = sentence('冗談ぬきでは話せない。', [
            tok('冗談', '冗談', ('名詞',)), tok('ぬき', '抜き', ('名詞',)),
            tok('で', 'で', ('助詞',)), tok('は', 'は', ('助詞',)),
            tok('話せない。', '話せる', ('動詞',)),
        ])
        stage = GrammarStep(rules=(SHORT_RULE, RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ぬき', 'で', 'は'])
        words = {'冗談': object()}
        stage.add_cards(words)
        self.assertIn('冗談', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ぬきでは')
        self.assertEqual(words[SHORT_RULE.expression].sentences[0].surface_form, 'ぬきで')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(
            LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression
        )
