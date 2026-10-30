import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import TE_KURU_GRAMMAR_EXPRESSION as EXPRESSION
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.te_kuru import match_te_kuru
from tests.test_grammar_te_iru import sentence, tok


class TeKuruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降ってくる。': 'てくる', '雨が降ってきた。': 'てきた',
            '雨が降ってきます。': 'てきます', '雨が降ってきました。': 'てきました',
            '雨が降ってきませんでした。': 'てきませんでした',
            '雨が降ってこない。': 'てこない', '雨が降ってこなかった。': 'てこなかった',
            '泳いでこなきゃならない。': 'でこなきゃ',
            '入ってこんかったがのう。': 'てこんかっ',
            '持って来る。': 'て来る', '持って来ない。': 'て来ない',
            '持ってきて。': 'てきて', '持ってこよう。': 'てこよう',
            '家に来た。': None, '連れてく。': None, '持ってくる。': 'てくる',
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_kuru(aligned)],
                                 [] if expected is None else [expected])

    def test_card_preserves_lexical_candidates_and_source_span(self):
        source = sentence('外では雨が降ってきたよ。', [
            tok('外では雨が', '雨', ('名詞',)),
            tok('降っ', '降る', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('き', 'くる', ('動詞', '非自立可能')),
            tok('た', 'た', ('助動詞',)), tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'降る': object(), 'くる': object()}
        stage.add_cards(words)
        self.assertIn('降る', words)
        self.assertIn('くる', words)
        self.assertEqual(words[EXPRESSION].frequency, 2)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, 'てきた')
        self.assertEqual(len(words[EXPRESSION].sentences), 1)

    def test_incomplete_or_gapped_tokens_do_not_match(self):
        base = [dict(surface='降っ', lemma='降る', pos=['動詞', '*', '*', '*', '*', '連用形'], span=[0, 2]),
                dict(surface='て', lemma='て', pos=['助詞', '接続助詞'], span=[2, 3]),
                dict(surface='き', lemma='くる', pos=['動詞', '非自立可能'], span=[3, 4])]
        self.assertEqual(list(match_te_kuru(base)), [])
        base[-1]['span'] = [4, 5]
        self.assertEqual(list(match_te_kuru(base)), [])


if __name__ == '__main__':
    unittest.main()
