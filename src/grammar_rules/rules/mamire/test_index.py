import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.mamire.index import MAMIRE_EXPRESSION, match_mamire
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MamireTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'紙まみれの部屋': 'まみれ', '嘘まみれになった': 'まみれ',
                 'まみれた': None, '花にまみれた': None, 'まみれ': None,
                 '泥まみれ': None, '血塗れ': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_mamire(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_pos(self):
        noun = tok('泥', '泥', ('名詞',))
        suffix = tok('まみれ', 'まみれ', ('接尾辞', '名詞的'))
        for text, raw, expected in [
            ('泥まみれ', [noun, suffix], ['まみれ']),
            ('泥 まみれ', [noun, suffix], []),
            ('泥まみれ', [noun, tok('まみれ', 'まみれる', ('動詞',))], []),
            ('泥まみれ', [tok('泥', '泥', ('動詞',)), suffix], []),
        ]:
            with self.subTest(text=text, raw=raw):
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_mamire(tokens)], expected)

    def test_card_and_reservation_preserve_noun(self):
        source = sentence('紙まみれの部屋を片付けた。', [tok('紙', '紙', ('名詞',)),
                          tok('まみれ', 'まみれ', ('接尾辞', '名詞的')),
                          tok('の部屋を片付けた。', 'の', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'紙': object()}
        stage.add_cards(words)
        self.assertIn('紙', words)
        self.assertEqual(words[MAMIRE_EXPRESSION].frequency, 2)
        self.assertEqual(words[MAMIRE_EXPRESSION].sentences[0].surface_form, 'まみれ')
        result = ExcludeGrammarTokensStep(stage).process(Artifact([source, source]))
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved', False))
        self.assertTrue(result.data[0].tokens[1].get('grammar_reserved', False))

    def test_whole_compound_remains_lexical(self):
        source = sentence('泥まみれになった。', [
            tok('泥まみれ', '泥まみれ', ('名詞', '普通名詞')),
            tok('になった。', 'だ', ('助動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {}
        stage.add_cards(words)
        self.assertNotIn(MAMIRE_EXPRESSION, words)
        result = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved', False))
