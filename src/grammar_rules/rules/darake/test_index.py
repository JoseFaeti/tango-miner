import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.darake.index import DARAKE_EXPRESSION, match_darake
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DarakeTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'泥だらけの服': 'だらけ', '間違いだらけです': 'だらけ',
                 '傷だらけになった': 'だらけ', 'ゴミだらけ': 'だらけ',
                 'だらける': None, 'だらけ': None, '血塗れ': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_darake(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_pos(self):
        noun = tok('泥', '泥', ('名詞',))
        suffix = tok('だらけ', 'だらけ', ('接尾辞', '形状詞的'))
        for text, raw, expected in [
            ('泥だらけ', [noun, suffix], ['だらけ']),
            ('泥 だらけ', [noun, suffix], []),
            ('泥だらけ', [noun, tok('だらけ', 'だらける', ('動詞',))], []),
            ('泥だらけ', [tok('泥', '泥', ('動詞',)), suffix], []),
        ]:
            with self.subTest(text=text, raw=raw):
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_darake(tokens)], expected)

    def test_card_preserves_noun_and_source(self):
        source = sentence('泥だらけの服を洗いました。', [tok('泥', '泥', ('名詞',)),
                          tok('だらけ', 'だらけ', ('接尾辞', '形状詞的')),
                          tok('の服を洗いました。', 'の', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'泥': object()}
        stage.add_cards(words)
        self.assertIn('泥', words)
        self.assertEqual(words[DARAKE_EXPRESSION].frequency, 2)
        self.assertEqual(words[DARAKE_EXPRESSION].sentences[0].surface_form, 'だらけ')
        self.assertEqual(len(words[DARAKE_EXPRESSION].sentences), 1)
