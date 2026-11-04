import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import TE_HOSHII_GRAMMAR_EXPRESSION as EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_hoshii.index import match_te_hoshii
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TeHoshiiTests(unittest.TestCase):
    def sudachi_tokens(self, text):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        return [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]

    def test_sudachi_forms_and_lexical_controls(self):
        cases = {
            '私は本を読んでほしいと思います。': 'でほしい',
            '彼に見て欲しかったと思う。': 'て欲しかった',
            '彼に読んでほしくないと思う。': 'でほしくない',
            '彼に見てほしければ頼もう。': 'てほしければ',
            '本を読んでほしいです。': 'でほしいです',
            '新しい本が欲しいです。': None,
            '本を読んでいます。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                tokens, error = aligned_tokens(text, self.sudachi_tokens(text))
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_hoshii(tokens)],
                                 [] if expected is None else [expected])

    def test_forms_and_controls(self):
        verb = tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        connective = tok('で', 'で', ('助詞', '接続助詞'))
        adjective = lambda surface: tok(surface, '欲しい', ('形容詞', '非自立可能'))
        cases = [
            ([verb, connective, adjective('ほしい')], ['でほしい']),
            ([verb, connective, adjective('欲しかっ'), tok('た', 'た', ('助動詞',))], ['で欲しかった']),
            ([verb, connective, adjective('ほしく'), tok('ない', 'ない', ('形容詞',))], ['でほしくない']),
            ([verb, connective, adjective('ほしけれ'), tok('ば', 'ば', ('助詞',))], ['でほしければ']),
            ([verb, connective, adjective('ほしい'), tok('です', 'です', ('助動詞',))], ['でほしいです']),
            ([verb, connective, adjective('ほしかっ')], []),
            ([tok('本', '本', ('名詞',)), connective, adjective('ほしい')], []),
            ([verb, tok('で', 'だ', ('助動詞',)), adjective('ほしい')], []),
            ([verb, connective, tok('ほしい', 'ほしい', ('名詞',))], []),
        ]
        for parts, expected in cases:
            with self.subTest(expected=expected, parts=parts):
                text = ''.join(part['surface'] for part in parts)
                tokens, error = aligned_tokens(text, parts)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_hoshii(tokens)], expected)

    def test_card_keeps_lexical_candidate(self):
        text = '私は本を読んでほしいと思います。'
        source = sentence(text, self.sudachi_tokens(text))
        stage = GrammarStep()
        artifact = Artifact([source])
        stage.process(artifact)
        ExcludeGrammarTokensStep(stage).process(artifact)
        reserved = [token['surface'] for token in artifact.data[0].tokens
                    if token.get('grammar_reserved')]
        self.assertEqual(reserved, ['で', 'ほしい', 'と', '思い', 'ます'])
        self.assertFalse(next(token for token in artifact.data[0].tokens
                              if token['surface'] == '読ん').get('grammar_reserved', False))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, 'でほしい')


if __name__ == '__main__':
    unittest.main()
