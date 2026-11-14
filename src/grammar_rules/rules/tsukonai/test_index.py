import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tsukonai.index import TSUKONAI_EXPRESSION, match_tsukonai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TsukonaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼に勝ちっこない。': 'っこない',
            'そんなことできっこなかった。': 'っこなかった',
            '分かりっこなくて困る。': 'っこなくて',
            '行きっこないです。': 'っこないです',
            '鬼ごっこない。': None, '勝ちっこだ。': None,
            '彼は勝てない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tsukonai(aligned)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card(self):
        verb = tok('勝ち', '勝つ', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('っこ', 'っこ', ('接尾辞', '名詞的'))
        negative = tok('ない', 'ない', ('形容詞',))
        for text, raw in [
            ('勝ちっこなかっ', [verb, suffix, tok('なかっ', 'ない', ('形容詞',))]),
            ('勝ち っこない', [verb, suffix, negative]),
            ('猫っこない', [tok('猫', '猫', ('名詞',)), suffix, negative]),
        ]:
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_tsukonai(aligned)), [])
        source = sentence('あの人には勝ちっこないと思います。', [
            tok('あの人には', '人', ('名詞',)), verb, suffix, negative,
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'勝つ': object()}
        stage.add_cards(words)
        self.assertIn('勝つ', words)
        self.assertEqual(words[TSUKONAI_EXPRESSION].frequency, 2)
        self.assertEqual(words[TSUKONAI_EXPRESSION].sentences[0].surface_form, 'っこない')
