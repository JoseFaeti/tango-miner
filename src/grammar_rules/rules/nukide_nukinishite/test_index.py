import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nukide_nukinishite.index import (
    NUKIDE_NUKINISHITE_GRAMMAR_EXPRESSION as EXPRESSION,
)
from src.grammar_rules.rules.nukide_nukinishite.index import (
    match_nukide_nukinishite,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NukideNukinishiteTests(unittest.TestCase):
    def test_sudachi_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '砂糖抜きで飲む。': ['抜きで'],
            '冗談ぬきで話す。': ['ぬきで'],
            '彼抜きにして考える。': ['抜きにして'],
            '説明をぬきにして進める。': ['ぬきにして'],
            '手抜きで作る。': [], '追い抜きで勝つ。': [],
            '砂糖抜きにする。': [], '歯を抜いて帰る。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nukide_nukinishite(tokens)], expected)

    def test_boundaries_and_card_integration(self):
        source = sentence('私は砂糖抜きでお茶を飲みます。', [
            tok('私は', '私', ('代名詞',)), tok('砂糖', '砂糖', ('名詞',)),
            tok('抜き', '抜き', ('名詞',)), tok('で', 'で', ('助詞',)),
            tok('お茶を飲みます。', '飲む', ('動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'砂糖': object()}
        stage.add_cards(words)
        self.assertIn('砂糖', words)
        self.assertEqual(words[EXPRESSION].frequency, 2)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, '抜きで')

        raw = [tok('抜き', '抜く', ('動詞',)), tok('で', 'で', ('助詞',))]
        tokens, error = aligned_tokens('抜きで', raw)
        self.assertIsNone(error)
        self.assertEqual(list(match_nukide_nukinishite(tokens)), [])
