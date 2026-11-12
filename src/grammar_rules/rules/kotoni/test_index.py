import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import KOTONI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kotoni.index import match_kotoni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KotoniTests(unittest.TestCase):
    def test_sudachi_reactions_and_nominal_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '嬉しいことに、合格した。': 'ことに',
            '残念なことに雨が降った。': 'ことに',
            '驚いたことに、彼が来た。': 'ことに',
            '悲しい事に彼は去った。': '事に',
            '大切なことに気づく。': None,
            '行くことにした。': None,
            '行くことになる。': None,
            '嬉しいことがあった。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kotoni(aligned)],
                                 [] if expected is None else [expected])

    def test_card_preserves_lexical_candidate(self):
        source = sentence('嬉しいことに、合格した。', [
            tok('嬉しい', '嬉しい', ('形容詞', '一般', '*', '*', '形容詞', '連体形-一般')),
            tok('こと', 'こと', ('名詞', '普通名詞')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('、合格した。', '合格', ('名詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {'嬉しい': object()}
        stage.add_cards(words)
        self.assertIn('嬉しい', words)
        self.assertEqual(words[KOTONI_EXPRESSION].sentences[0].surface_form, 'ことに')


if __name__ == '__main__':
    unittest.main()
