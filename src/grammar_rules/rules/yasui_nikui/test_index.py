import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import YASUI_NIKUI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.yasui_nikui.index import match_yasui_nikui


class YasuiNikuiTests(unittest.TestCase):
    def test_sudachi_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '読みやすい': 'やすい', '読みにくかった': 'にくかった',
            '読みやすくない': 'やすくない', '読みにくくなかった': 'にくくなかった',
            '読みやすければ': 'やすければ', '読みにくくて': 'にくくて',
            '読みやすくなる': 'やすく', '読みやすいです': 'やすいです',
            '分かり易い': '易い', '行き難い': '難い',
            '安い': None, '醜い': None, '難しい': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_yasui_nikui(aligned)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_source_evidence(self):
        verb = tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('やすい', 'やすい', ('接尾辞', '形容詞的'))
        source = sentence('この本は読みやすいです。', [
            tok('この本は', '本', ('名詞',)), verb, suffix,
            tok('です。', 'です', ('助動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[YASUI_NIKUI_EXPRESSION].frequency, 2)
        self.assertEqual(words[YASUI_NIKUI_EXPRESSION].sentences[0].surface_form, 'やすい')
        for text, raw in [
            ('読み やすい', [verb, suffix]),
            ('読めやすい', [tok('読め', '読む', ('動詞', '一般', '*', '*', '五段', '仮定形')), suffix]),
            ('読みやすい', [verb, tok('やすい', 'やすい', ('形容詞', '一般'))]),
            ('読みやすかっ', [verb, tok('やすかっ', 'やすい', ('接尾辞', '形容詞的'))]),
        ]:
            with self.subTest(text=text):
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual(list(match_yasui_nikui(aligned)), [])
