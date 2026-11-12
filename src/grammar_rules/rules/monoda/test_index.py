import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import MONODA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.monoda.index import RULE, match_monoda
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MonodaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '人は誰でも死ぬものだ。': 'ものだ',
            '子供の頃よく遊んだものだ。': 'ものだ',
            '親の言うことは聞くものです。': 'ものです',
            '昔はよく遊んだものだった。': 'ものだった',
            '時間は大切なものです。': 'ものです',
            'これは私のものだ。': None,
            '買うものの名前。': None,
            '買うものだから困る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_monoda(aligned)],
                                 [] if expected is None else [expected])

    def test_card_and_complete_boundaries(self):
        verb = tok('遊んだ', '遊ぶ', ('動詞', '*', '*', '*', '*', '連体形-一般'))
        mono = tok('もの', 'もの', ('名詞', '普通名詞'))
        copula = tok('だ', 'だ', ('助動詞',))
        source = sentence('昔はよく遊んだものだ。', [tok('昔はよく', '昔', ('名詞',)), verb, mono, copula, tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(any(t.get('grammar_reserved') for t in reserved.data[0].tokens[1:4]))
        words = {'遊ぶ': object(), 'もの': object()}
        stage.add_cards(words)
        self.assertIn('遊ぶ', words)
        self.assertIn('もの', words)
        self.assertEqual(words[MONODA_EXPRESSION].frequency, 2)
        self.assertEqual(words[MONODA_EXPRESSION].sentences[0].surface_form, 'ものだ')
        self.assertEqual(len(words[MONODA_EXPRESSION].sentences), 1)
        for text, raw in [('遊んだものだっ', [verb, mono, tok('だっ', 'だ', ('助動詞',))]),
                          ('遊んだ ものだ', [verb, mono, copula])]:
            aligned, error = aligned_tokens(text, raw)
            if error is None:
                self.assertEqual(list(match_monoda(aligned)), [])


if __name__ == '__main__':
    unittest.main()
