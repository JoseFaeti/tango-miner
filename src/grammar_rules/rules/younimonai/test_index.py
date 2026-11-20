import unittest

from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import tok
from src.grammar_rules.rules.younimonai.index import RULE, match_younimonai


class YounimonaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行こうにも行けない。': '行こうにも行けない',
            '食べようにも食べられない。': '食べようにも食べられない',
            'しようにもできない。': 'しようにもできない',
            '来ようにも来られなかった。': '来ようにも来られなかった',
            '帰ろうにも帰れません。': '帰ろうにも帰れません',
            '行こうと思う。': None, '鳥のようにも見えない。': None,
            '行こうにも行ける。': None, '行こうにも 行けない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_younimonai(aligned)],
                                 [] if expected is None else [expected])

    def test_registered_identity(self):
        self.assertIn(RULE, RULES)
        self.assertEqual(RULE.expression, '〜ようにも〜ない')
