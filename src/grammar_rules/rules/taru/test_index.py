import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.taru.index import RULE, match_taru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class TaruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'教師たる者、責任を持て。': ['たる'], '国民の代表たる議員。': ['たる'],
                 '堂々たる態度。': [], '議論に足る価値。': [], '水が足る。': [],
                 '滴る水。': [], '王たるや威厳がある。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_taru(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('教師たる者、責任を持て。', [tok('教師', '教師', ('名詞',)),
                          tok('たる', 'たり', ('助動詞',)), tok('者', '者', ('名詞',)),
                          tok('、責任を持て。', '責任', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['たる'])
        words = {'教師': object()}
        stage.add_cards(words)
        self.assertIn('教師', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'たる')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
