import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.mai.index import MAI_EXPRESSION, match_mai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MaiCardTests(unittest.TestCase):
    def test_sudachi_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        for text, expected in {
            'もう行くまい。': True, '二度と食べまい。': True,
            '彼は食べるまい。': True, 'もうするまい。': True,
            '彼は来まい。': True, '彼は来るまい。': True,
            'そんなことはあるまい。': True, '行きますまい。': True,
            'うまい料理だ。': False, '姉妹が住まいを探す。': False,
            '舞い上がる。': False,
        }.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_mai(aligned)],
                                 ['まい'] if expected else [])

    def test_boundaries_and_card_integration(self):
        verb = tok('行く', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '終止形-一般'))
        mai = tok('まい', 'まい', ('助動詞',))
        source = sentence('私はもう二度と行くまいと思います。', [
            tok('私はもう二度と', '二度と', ('副詞',)), verb, mai,
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[MAI_EXPRESSION].sentences[0].surface_form, 'まい')
        for text, raw in [('行く まい', [verb, mai]),
                          ('行くまい', [verb, tok('まい', 'まい', ('名詞',))])]:
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_mai(aligned)), [])


if __name__ == '__main__':
    unittest.main()
