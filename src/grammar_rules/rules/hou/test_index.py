import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import HOU_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.hou.index import match_hou
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class HouTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        for text, expected in {
            '食べ方が分からない。': ['方'], '読みかたを教える。': ['かた'],
            '行く方がいい。': [], '味方がいる。': [], '本の読み方。': [],
        }.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_hou(tokens)], expected)

    def test_boundaries_and_source_evidence(self):
        verb = tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '連用形-一般'))
        suffix = tok('方', '方', ('接尾辞', '名詞的'))
        source = sentence('私は食べ方を教えています。', [tok('私は', '私', ('名詞',)),
                          verb, suffix, tok('を教えています。', 'を', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertEqual(words[HOU_EXPRESSION].frequency, 2)
        self.assertEqual(words[HOU_EXPRESSION].sentences[0].surface_form, '方')
        artifact = Artifact([source, source])
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[2].get('grammar_reserved', False))
        for text, raw in [
            ('食べ 方', [verb, suffix]),
            ('食べ方', [verb, tok('方', '方', ('名詞',))]),
            ('食べた方', [verb, tok('た', 'た', ('助動詞',)), suffix]),
        ]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_hou(tokens)), [])


if __name__ == '__main__':
    unittest.main()
