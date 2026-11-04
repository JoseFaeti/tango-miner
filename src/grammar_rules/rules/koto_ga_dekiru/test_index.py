import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import KOTO_GA_DEKIRU_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.koto_ga_dekiru.index import match_koto_ga_dekiru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KotoGaDekiruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読むことができる。': 'ことができる',
            '本を読むことができた。': 'ことができた',
            '本を読むことができない。': 'ことができない',
            '本を読むことができなかった。': 'ことができなかった',
            '本を読むことができます。': 'ことができます',
            '本を読むことができませんでした。': 'ことができませんでした',
            '本を読む事が出来ます。': '事が出来ます',
            '仕事ができる。': None, '読むことが大切だ。': None,
            '本を読むことができて嬉しい。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_koto_ga_dekiru(aligned)],
                                 [] if expected is None else [expected])

    def test_card_preserves_verb(self):
        raw = [tok('私は本を', '本', ('名詞',)),
               tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
               tok('こと', 'こと', ('名詞',)), tok('が', 'が', ('助詞',)),
               tok('できる', 'できる', ('動詞',)), tok('と思います。', 'と', ('助詞',))]
        source = sentence('私は本を読むことができると思います。', raw)
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[KOTO_GA_DEKIRU_EXPRESSION].sentences[0].surface_form, 'ことができる')


if __name__ == '__main__':
    unittest.main()
