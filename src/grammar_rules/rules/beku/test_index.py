import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.beku.index import BEKU_EXPRESSION, match_beku
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class BekuCardTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        for text, expected in {
            '合格するべく勉強する。': True, '合格すべく勉強する。': True,
            '行くべく出発した。': True, '食べるべく準備した。': True,
            '来るべく努力する。': True, 'なるべく早く帰る。': False,
            '起こるべくして起こった。': False, '知るべくもない。': False,
        }.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_beku(aligned)],
                                 ['べく'] if expected else [])

    def test_boundaries_and_card_integration(self):
        verb = tok('行く', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '終止形-一般'))
        beku = tok('べく', 'べし', ('助動詞',))
        source = sentence('目的地へ行くべく準備をした。', [
            tok('目的地へ', '目的地', ('名詞',)), verb, beku,
            tok('準備をした。', '準備', ('名詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[BEKU_EXPRESSION].sentences[0].surface_form, 'べく')
        for text, raw in [('行く べく', [verb, beku]),
                          ('行くべく', [verb, tok('べく', 'べく', ('名詞',))])]:
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_beku(aligned)), [])


if __name__ == '__main__':
    unittest.main()
