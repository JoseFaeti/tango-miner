import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import TEMORAITAI_EXPRESSION, TAI_GRAMMAR_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.temoraitai.index import match_temoraitai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TemoraitaiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            from sudachipy import dictionary, tokenizer
            cls.analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
            cls.mode = tokenizer.Tokenizer.SplitMode.C
        except (ImportError, OSError, RuntimeError) as exc:
            raise unittest.SkipTest(f'Sudachi full dictionary unavailable: {exc}')

    def tokens(self, text):
        return [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                for m in self.analyzer.tokenize(text, self.mode)]

    def test_forms_and_controls(self):
        cases = {
            '彼に本を読んでもらいたい。': 'でもらいたい',
            '彼に見てもらいたかった。': 'てもらいたかった',
            '彼に見てもらいたくない。': 'てもらいたくない',
            '彼に見てもらいたければ頼む。': 'てもらいたければ',
            '彼に見てもらいたいです。': 'てもらいたいです',
            '本をもらいたい。': None,
            '彼に見てあげたい。': None,
            '彼に見てもらった。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                tokens, error = aligned_tokens(text, self.tokens(text))
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_temoraitai(tokens)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation_keep_verb_and_overlap(self):
        text = '彼に本を読んでもらいたいと思います。'
        artifact = Artifact([sentence(text, self.tokens(text))])
        stage = GrammarStep()
        stage.process(artifact)
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[TEMORAITAI_EXPRESSION].sentences[0].surface_form, 'でもらいたい')
        self.assertIn(TAI_GRAMMAR_EXPRESSION, words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        tokens = artifact.data[0].tokens
        self.assertFalse(next(t for t in tokens if t['surface'] == '読ん').get('grammar_reserved', False))
        for surface in ('で', 'もらい', 'たい'):
            self.assertTrue(next(t for t in tokens if t['surface'] == surface).get('grammar_reserved'))

    def test_requires_complete_adjacent_morphology(self):
        verb = tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        connective = tok('で', 'で', ('助詞', '接続助詞'))
        auxiliary = tok('もらい', 'もらう', ('動詞', '非自立可能', '*', '*', '五段', '連用形-一般'))
        desire = tok('たい', 'たい', ('助動詞',))
        for parts in ([tok('本', '本', ('名詞',)), connective, auxiliary, desire],
                      [verb, tok('で', 'だ', ('助動詞',)), auxiliary, desire],
                      [verb, connective, tok('もらい', 'もらう', ('動詞', '一般')), desire],
                      [verb, connective, auxiliary, tok('たかっ', 'たい', ('助動詞',))]):
            with self.subTest(parts=parts):
                text = ''.join(t['surface'] for t in parts)
                tokens, error = aligned_tokens(text, parts)
                self.assertIsNone(error)
                self.assertEqual(list(match_temoraitai(tokens)), [])


if __name__ == '__main__':
    unittest.main()
