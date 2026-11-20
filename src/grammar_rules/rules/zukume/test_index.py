import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.zukume.index import ZUKUME_EXPRESSION, match_zukume


class ZukumeTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'いいことずくめ': 'ずくめ', '黒ずくめの服': 'ずくめ',
                 '規則ずくめだ': 'ずくめ', '黒尽くめ': '尽くめ',
                 'ずくめ': None, 'ずくめる': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zukume(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_pos(self):
        noun = tok('黒', '黒', ('名詞',))
        suffix = tok('ずくめ', 'ずくめ', ('接尾辞', '名詞的'))
        for text, raw, expected in [
            ('黒ずくめ', [noun, suffix], ['ずくめ']),
            ('黒 ずくめ', [noun, suffix], []),
            ('黒ずくめ', [noun, tok('ずくめ', 'ずくめる', ('動詞',))], []),
            ('黒ずくめ', [tok('黒', '黒', ('動詞',)), suffix], []),
        ]:
            with self.subTest(text=text, raw=raw):
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zukume(tokens)], expected)

    def test_card_preserves_noun_and_source(self):
        source = sentence('黒ずくめの服を着ていました。', [tok('黒', '黒', ('名詞',)),
                          tok('ずくめ', 'ずくめ', ('接尾辞', '名詞的')),
                          tok('の服を着ていました。', 'の', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'黒': object()}
        stage.add_cards(words)
        self.assertIn('黒', words)
        self.assertEqual(words[ZUKUME_EXPRESSION].frequency, 2)
        self.assertEqual(words[ZUKUME_EXPRESSION].sentences[0].surface_form, 'ずくめ')
        self.assertEqual(len(words[ZUKUME_EXPRESSION].sentences), 1)
