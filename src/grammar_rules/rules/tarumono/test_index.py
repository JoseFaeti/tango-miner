import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tarumono.index import RULE, match_tarumono
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TarumonoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'教師たる者、責任を持て。': ['たる者'],
                 '教師たるものは勉強すべきだ。': ['たるもの'],
                 '国民の代表たる議員。': [], '堂々たる態度。': [],
                 '水が足る。': [], '議論に足るものがない。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tarumono(tokens)], expected)

    def test_reservation_identity_and_overlap(self):
        source = sentence('教師たる者、責任を持て。', [tok('教師', '教師', ('名詞',)),
                          tok('たる', 'たり', ('助動詞',)), tok('者', '者', ('名詞',)),
                          tok('、責任を持て。', '責任', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['たる', '者'])
        words = {'教師': object()}
        stage.add_cards(words)
        self.assertIn('教師', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'たる者')
        self.assertIn('〜たる', words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
