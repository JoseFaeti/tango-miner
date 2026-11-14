import unittest

from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import tok
from src.grammar_rules.rules.youganai.index import match_youganai, RULE
from src.grammar_rules.registry import RULES


class YouganaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は食べようがない。': 'ようがない',
            '説明しようがなかった。': 'ようがなかった',
            '言いようもない。': 'ようもない',
            '読みようがなくて困る。': 'ようがなくて',
            '読みようがなければ困る。': 'ようがなければ',
            '読みようがないです。': 'ようがないです',
            '仕様がない。': None, '鳥のようがない。': None,
            '知るようがない。': None, '食べようと思う。': None,
            '食べようが ない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_youganai(aligned)],
                                 [] if expected is None else [expected])

    def test_registered_identity(self):
        self.assertIn(RULE, RULES)
        self.assertEqual(RULE.expression, '〜ようがない')
