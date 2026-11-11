import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import SUGIRU_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.sugiru.index import match_sugiru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class SugiruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '食べすぎる': '食べすぎる', '飲み過ぎた': '過ぎた',
            '高すぎる': 'すぎる', '静かすぎます': 'すぎます',
            '多すぎない': 'すぎない', '食べすぎて': '食べすぎて',
            '食べすぎれば': '食べすぎれば', 'なさすぎる': 'すぎる',
            '時間が過ぎる': None, '過ぎた': None, '通り過ぎる': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_sugiru(tokens)],
                                 [] if expected is None else [expected])

    def test_incomplete_and_nonadjacent_forms(self):
        verb = tok('飲み', '飲む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('すぎ', 'すぎる', ('動詞', '非自立可能'))
        for raw in ([verb, suffix], [verb, tok('を', 'を', ('助詞',)), suffix]):
            text = ''.join(t['surface'] for t in raw)
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_sugiru(tokens)), [])

    def test_card_keeps_lexical_candidate(self):
        source = sentence('私はお酒を飲みすぎると思います。', [
            tok('私はお酒を', '酒', ('名詞',)),
            tok('飲み', '飲む', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('すぎる', 'すぎる', ('動詞', '非自立可能')), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'飲む': object()}
        stage.add_cards(words)
        self.assertIn('飲む', words)
        self.assertEqual(words[SUGIRU_EXPRESSION].frequency, 2)
        self.assertEqual(words[SUGIRU_EXPRESSION].sentences[0].surface_form, 'すぎる')


if __name__ == '__main__':
    unittest.main()
