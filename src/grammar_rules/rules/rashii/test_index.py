import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import RASHII_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.rashii.index import match_rashii
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class RashiiTests(unittest.TestCase):
    def test_sudachi_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降るらしい。': 'らしい', '雨だったらしい。': 'らしい',
            '静からしい。': 'らしい', '学生らしいです。': 'らしいです',
            '子供らしかった。': 'らしかった', '彼女らしくない。': 'らしくない',
            '行くらしくて困る。': 'らしくて', '男らしければいい。': 'らしければ',
            '素晴らしい。': None, '新しい。': None, 'らしいね。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_rashii(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_incomplete_forms(self):
        noun = tok('子供', '子供', ('名詞',))
        suffix = tok('らしい', 'らしい', ('助動詞',))
        source = sentence('子供らしいと思います。', [noun, suffix, tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'子供': object()}
        stage.add_cards(words)
        self.assertIn('子供', words)
        self.assertEqual(words[RASHII_EXPRESSION].frequency, 2)
        self.assertEqual(words[RASHII_EXPRESSION].sentences[0].surface_form, 'らしい')
        result = ExcludeGrammarTokensStep(stage).process(Artifact([source, source]))
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved', False))
        self.assertTrue(result.data[0].tokens[1].get('grammar_reserved', False))
        for parts in ([noun, tok('らしかっ', 'らしい', ('助動詞',))],
                      [noun, tok('らしい', 'らしい', ('名詞',))]):
            text = ''.join(t['surface'] for t in parts)
            aligned, error = aligned_tokens(text, parts)
            self.assertIsNone(error)
            self.assertEqual(list(match_rashii(aligned)), [])
