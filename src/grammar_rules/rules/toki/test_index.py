import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import TOKI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.toki.index import match_toki
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TokiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '日本へ行くときに会う。': 'とき', '日本へ行った時に会う。': '時',
            '行かないときは休む。': 'とき', '寒いときに飲む。': 'とき',
            '静かな時に読む。': '時', '子供のときに遊んだ。': 'とき',
            '時を忘れる。': None, '時計を見る。': None, '時々会う。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toki(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card_evidence(self):
        verb = tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '連体形-一般'))
        noun = tok('とき', '時', ('名詞', '普通名詞'))
        source = sentence('学校へ行くときに友達と会います。', [
            tok('学校へ', '学校', ('名詞',)), verb, noun,
            tok('に友達と会います。', 'に', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[TOKI_EXPRESSION].frequency, 2)
        self.assertEqual(words[TOKI_EXPRESSION].sentences[0].surface_form, 'とき')
        tokens, error = aligned_tokens('行く とき', [verb, noun])
        self.assertIsNone(error)
        self.assertEqual(list(match_toki(tokens)), [])


if __name__ == '__main__':
    unittest.main()
