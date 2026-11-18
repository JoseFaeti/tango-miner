import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.gatera.index import GATERA_EXPRESSION, match_gatera
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class GateraTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'散歩がてら買い物に行く': 'がてら', '買い物がてら': 'がてら',
                 '散歩をしがてら寄る': 'がてら', '行きがてら': 'がてら',
                 'がてら': None, 'ガテラ': None, '長寺': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_gatera(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card_evidence(self):
        verb = tok('行き', '行く', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('がてら', 'がてら', ('接尾辞', '名詞的'))
        for text, raw in [
            ('行き がてら', [verb, suffix]),
            ('行きがてら', [verb, tok('がてら', 'がてら', ('名詞',))]),
            ('行きがてら', [tok('行き', '行く', ('動詞', '一般', '*', '*', '五段', '仮定形')), suffix]),
            ('がてら', [suffix]),
        ]:
            with self.subTest(text=text):
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual(list(match_gatera(tokens)), [])
        source = sentence('駅へ行きがてら店に寄りました。', [
            tok('駅へ', '駅', ('名詞',)), verb, suffix,
            tok('店に寄りました。', '寄る', ('動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[GATERA_EXPRESSION].frequency, 2)
        self.assertEqual(words[GATERA_EXPRESSION].sentences[0].surface_form, 'がてら')
