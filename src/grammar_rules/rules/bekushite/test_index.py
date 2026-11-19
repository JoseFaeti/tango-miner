import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.bekushite.index import BEKUSHITE_EXPRESSION, match_bekushite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class BekushiteCardTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        for text, expected in {
            '起こるべくして起こった。': True, 'なるべくしてなった。': False,
            '失敗するべくして失敗した。': True, '合格すべくして合格した。': True,
            'なるべく早く帰る。': False, '行くべく出発した。': False,
            '知るべくもない。': False, '勉強して合格した。': False,
        }.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_bekushite(aligned)],
                                 ['べくして'] if expected else [])

    def test_boundaries_and_card_integration(self):
        verb = tok('起こる', '起こる', ('動詞', '一般', '*', '*', '五段-ラ行', '終止形-一般'))
        beku = tok('べく', 'べし', ('助動詞',))
        shi = tok('し', 'する', ('動詞', '非自立可能', '*', '*', 'サ行変格', '連用形-一般'))
        te = tok('て', 'て', ('助詞', '接続助詞'))
        text = '事件は起こるべくして起こった。'
        source = sentence(text, [tok('事件は', '事件', ('名詞',)), verb, beku, shi, te,
                                 tok('起こった。', '起こる', ('動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {'起こる': object()}
        stage.add_cards(words)
        self.assertIn('起こる', words)
        self.assertEqual(words[BEKUSHITE_EXPRESSION].sentences[0].surface_form, 'べくして')
        self.assertNotIn('〜べく', words)
        for text, raw in [('起こる べくして', [verb, beku, shi, te]),
                          ('起こるべく して', [verb, beku, shi, te]),
                          ('起こるべくして', [verb, beku, tok('し', 'し', ('名詞',)), te])]:
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_bekushite(aligned)), [])


if __name__ == '__main__':
    unittest.main()
