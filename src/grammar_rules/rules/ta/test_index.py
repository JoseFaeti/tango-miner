import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ta.index import TA_GRAMMAR_EXPRESSION, match_ta
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TaCardTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読んだ。': ['だ'], 'ご飯を食べた。': ['た'],
            '昨日は寒かった。': ['た'], '本を読まなかった。': ['た'],
            '本を読みました。': ['た'], '静かだった。': ['た'],
            '読んだ本。': ['だ'], '食べたことがある。': ['た'],
            '食べたら帰る。': [], '食べたりする。': [],
            '田を見た。': ['た'], 'ただの本。': [], '本を読む。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ta(tokens)], expected)

    def test_boundaries_and_identity(self):
        verb = tok('食べ', '食べる', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        past = tok('た', 'た', ('助動詞', '*', '*', '*', '助動詞-タ', '終止形-一般'))
        source = sentence('私は本を食べたと思います。', [tok('私は本を', '本', ('名詞',)), verb, past, tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertEqual(words[TA_GRAMMAR_EXPRESSION].frequency, 2)
        self.assertEqual(words[TA_GRAMMAR_EXPRESSION].sentences[0].surface_form, 'た')
        tokens, error = aligned_tokens('食べ た', [verb, past])
        self.assertIsNone(error)
        self.assertEqual(list(match_ta(tokens)), [])


if __name__ == '__main__':
    unittest.main()
