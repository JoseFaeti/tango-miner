import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nikui_zurai_gatai.index import (
    NIKUI_ZURAI_GATAI_EXPRESSION, match_nikui_zurai_gatai,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.yasui_nikui.index import YASUI_NIKUI_EXPRESSION


class DifficultyTests(unittest.TestCase):
    def test_sudachi_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '読みにくい': 'にくい', '読みにくかった': 'にくかった',
            '読みにくくない': 'にくくない', '読みにくければ': 'にくければ',
            '言いづらい': 'づらい', '言いづらかった': 'づらかった',
            '言いづらくない': 'づらくない', '言いづらくて': 'づらくて',
            '信じがたい': 'がたい', '信じがたかった': 'がたかった',
            '信じがたくない': 'がたくない', '信じがたければ': 'がたければ',
            '行き難い': '難い', '醜い': None, '辛い': None, '難しい': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikui_zurai_gatai(aligned)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_overlapping_card(self):
        verb = tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('にくい', 'にくい', ('接尾辞', '形容詞的'))
        source = sentence('この本は読みにくい。', [tok('この本は', '本', ('名詞',)), verb,
                                            suffix, tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        for expression in (NIKUI_ZURAI_GATAI_EXPRESSION, YASUI_NIKUI_EXPRESSION):
            self.assertEqual(words[expression].frequency, 2)
            self.assertEqual(words[expression].sentences[0].surface_form, 'にくい')
        for text, raw in [
            ('読み にくい', [verb, suffix]),
            ('読めにくい', [tok('読め', '読む', ('動詞', '一般', '*', '*', '五段', '仮定形')), suffix]),
            ('読みにくい', [verb, tok('にくい', 'にくい', ('形容詞', '一般'))]),
            ('読みづらかっ', [verb, tok('づらかっ', 'づらい', ('接尾辞', '形容詞的'))]),
        ]:
            with self.subTest(text=text):
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual(list(match_nikui_zurai_gatai(aligned)), [])
