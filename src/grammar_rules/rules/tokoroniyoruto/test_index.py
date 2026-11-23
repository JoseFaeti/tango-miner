import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niyoruto_niyoreba.index import RULE as INNER_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tokoroniyoruto.index import RULE, match_tokoroniyoruto
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class TokoroniyorutoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '聞いたところによると明日は雨だ。': 'ところによると',
            '調べた所によると本当だ。': '所によると',
            '聞くところによると彼は元気だ。': 'ところによると',
            'ところによると違う。': None,
            '聞いたところによれば本当だ。': None,
            '聞いたところに よると本当だ。': None,
            '彼が行ったところに寄ると店がある。': None,
            '天気予報によると雨だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tokoroniyoruto(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_reservation_identity_and_overlap(self):
        source = sentence('聞いたところによると雨だ。', [
            tok('聞い', '聞く', ('動詞',)),
            tok('た', 'た', ('助動詞', '*', '*', '*', '助動詞-タ', '連体形-一般')),
            tok('ところ', 'ところ', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('よる', 'よる', ('動詞',)), tok('と', 'と', ('助詞',)),
            tok('雨だ。', '雨', ('名詞',))])
        stage = GrammarStep(rules=(INNER_RULE, RULE))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ところ', 'に', 'よる', 'と'])
        words = {'聞く': object()}
        stage.add_cards(words)
        self.assertIn('聞く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ところによると')
        self.assertIn(INNER_RULE.expression, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
