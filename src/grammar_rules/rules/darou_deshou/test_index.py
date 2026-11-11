import unittest

from src.Grammar import GrammarStep
from src.Artifact import Artifact
from src.GrammarConstants import DAROU_DESHOU_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.darou_deshou.index import match_darou_deshou
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DarouDeshouTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'明日は雨だろう。': 'だろう', '行くでしょう。': 'でしょう',
                 'そうでしょ？': 'でしょ', 'だろうね。': 'だろう',
                 '行くんでしょう。': 'でしょう', '太郎だろうか。': 'だろう',
                 '太郎は行く。': None, '本です。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_darou_deshou(tokens)],
                                 [] if expected is None else [expected])

    def test_token_boundaries_and_identity(self):
        raw = [tok('だろう', 'だ', ('名詞',)), tok('でしょう', 'です', ('助動詞',))]
        tokens, error = aligned_tokens('だろうでしょう', raw)
        self.assertIsNone(error)
        self.assertEqual(list(match_darou_deshou(tokens)), [(3, 7)])
        source = sentence('明日は雨だろうと思います。', [tok('明日は雨', '雨', ('名詞',)),
                          tok('だろう', 'だ', ('助動詞',)), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'雨': object()}
        stage.add_cards(words)
        self.assertIn('雨', words)
        self.assertEqual(words[DAROU_DESHOU_EXPRESSION].sentences[0].surface_form, 'だろう')


if __name__ == '__main__':
    unittest.main()
