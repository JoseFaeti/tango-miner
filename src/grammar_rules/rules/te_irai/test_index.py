import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.irai.index import IRAI_EXPRESSION
from src.grammar_rules.rules.te_irai.index import RULE, match_te_irai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TeIraiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '日本に来て以来住んでいる。': 'て以来',
            '本を読んで以来会っていない。': 'で以来',
            '卒業して以来会っていない。': 'て以来',
            '卒業以来会っていない。': None, '依頼を受ける。': None,
            '手で依頼する。': None, '来ていらい会っていない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_irai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_boundaries(self):
        raw = [tok('来', '来る', ('動詞', '非自立可能', '*', '*', '*', '連用形-一般')),
               tok('て', 'て', ('助詞', '接続助詞')), tok('以来', '以来', ('名詞',)),
               tok('会っていない。', '会う', ('動詞',))]
        source = sentence('来て以来会っていない。', raw)
        stage = GrammarStep()
        data = stage.process(Artifact([source]))
        data = ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['て', '以来'])
        words = {'来る': object()}
        stage.add_cards(words)
        self.assertIn('来る', words)
        self.assertNotIn(IRAI_EXPRESSION, words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'て以来')
        tokens, error = aligned_tokens(source.text, source.tokens)
        self.assertIsNone(error)
        tokens[2]['span'] = [tokens[2]['span'][0] + 1, tokens[2]['span'][1]]
        self.assertEqual(list(match_te_irai(tokens)), [])


if __name__ == '__main__':
    unittest.main()
