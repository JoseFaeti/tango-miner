import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import MAENI_ATODE_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.maeni_atode.index import match_maeni_atode
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MaeniAtodeTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '食べる前に手を洗う。': '前に', '読んだあとで話す。': 'あとで',
            '食べた後で寝る。': '後で', '食事の前に帰る。': '前に',
            '食事の後で帰る。': '後で', '後で行く。': None,
            '前に進む。': None, '食べる後で寝る。': None,
            '食べた前に帰る。': None, 'その後で帰る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_maeni_atode(aligned)],
                                 [] if expected is None else [expected])

    def test_card_retains_attached_lexical_evidence(self):
        source = sentence('私は食事の前に家へ帰ります。', [
            tok('私は', '私', ('名詞',)), tok('食事', '食事', ('名詞',)),
            tok('の', 'の', ('助詞', '格助詞')),
            tok('前', '前', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
            tok('家へ帰ります。', '帰る', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['前', 'に'])
        words = {'食事': object()}
        stage.add_cards(words)
        self.assertIn('食事', words)
        self.assertEqual(words[MAENI_ATODE_EXPRESSION].frequency, 2)
        self.assertEqual(words[MAENI_ATODE_EXPRESSION].sentences[0].surface_form, '前に')


if __name__ == '__main__':
    unittest.main()
