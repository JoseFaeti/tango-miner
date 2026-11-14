import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.gachi.index import GACHI_EXPRESSION, match_gachi
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class GachiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'忘れがち': 'がち', '病気がちな人': 'がち',
                 '留守がちです': 'がち', 'ありがち': 'がち',
                 '勝ちがち': 'がち', 'ガチ': None, 'がちゃ': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_gachi(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card_evidence(self):
        verb = tok('忘れ', '忘れる', ('動詞', '一般', '*', '*', '下一段', '連用形-一般'))
        suffix = tok('がち', 'がち', ('接尾辞', '形状詞的'))
        for text, raw in [
            ('忘れ がち', [verb, suffix]),
            ('忘れがち', [verb, tok('がち', 'がち', ('名詞',))]),
            ('忘れがち', [tok('忘れ', '忘れる', ('動詞', '一般', '*', '*', '下一段', '仮定形')), suffix]),
            ('がち', [suffix]),
        ]:
            with self.subTest(text=text):
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual(list(match_gachi(tokens)), [])
        source = sentence('大事な約束を忘れがちです。', [
            tok('大事な約束を', '約束', ('名詞',)), verb, suffix,
            tok('です。', 'です', ('助動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'忘れる': object()}
        stage.add_cards(words)
        self.assertIn('忘れる', words)
        self.assertEqual(words[GACHI_EXPRESSION].frequency, 2)
        self.assertEqual(words[GACHI_EXPRESSION].sentences[0].surface_form, 'がち')
