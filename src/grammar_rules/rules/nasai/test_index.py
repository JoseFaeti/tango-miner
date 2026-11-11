import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import NASAI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nasai.index import match_nasai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NasaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '早く食べなさい。': ['なさい'], '本を読みなさい。': ['なさい'],
            '勉強しなさい。': ['なさい'], 'こちらへ来なさい。': ['なさい'],
            '行きなさいよ。': ['なさい'], 'おやすみなさい。': [],
            'ごめんなさい。': [], '話をなさい。': [],
            '食べなさいます。': [], '食べなさった。': [],
            'お帰りなさい。': ['なさい'],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nasai(aligned)], expected)

    def test_boundaries_and_morphology(self):
        verb = tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        command = tok('なさい', 'なさる', ('動詞', '非自立可能', '*', '*', '五段', '命令形'))
        for text, raw in [
            ('読み なさい', [verb, command]),
            ('読みなさい', [tok('読み', '読む', ('名詞',)), command]),
            ('読みなさい', [verb, tok('なさい', 'なさる', ('動詞', '非自立可能', '*', '*', '五段', '連用形-イ音便'))]),
        ]:
            with self.subTest(text=text, raw=raw):
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual(list(match_nasai(aligned)), [])

    def test_card_keeps_lexical_candidate_and_source(self):
        source = sentence('あなたはこの本を読みなさい。', [
            tok('あなたはこの本を', '本', ('名詞',)),
            tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('なさい', 'なさる', ('動詞', '非自立可能', '*', '*', '五段', '命令形')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[NASAI_EXPRESSION].frequency, 2)
        self.assertEqual(words[NASAI_EXPRESSION].sentences[0].surface_form, 'なさい')


if __name__ == '__main__':
    unittest.main()
