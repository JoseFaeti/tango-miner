import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ageku.index import AGEKU_EXPRESSION, match_ageku
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class AgekuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '散々迷ったあげく、諦めた。': 'あげく',
            '悩んだ挙句に諦めた。': '挙句に',
            '議論の挙句に決めた。': '挙句に',
            '物を上げ句を読む。': None,
            'あげくに失敗した。': None,
            '考えた挙げ句の果てに負けた。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ageku(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_evidence(self):
        source = sentence('迷ったあげくに諦めた。', [
            tok('迷っ', '迷う', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('あげく', 'あげく', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('諦めた。', '諦める', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['あげく', 'に'])
        words = {'迷う': object()}
        stage.add_cards(words)
        self.assertIn('迷う', words)
        self.assertEqual(words[AGEKU_EXPRESSION].frequency, 2)
        self.assertEqual(words[AGEKU_EXPRESSION].sentences[0].surface_form, 'あげくに')

    def test_token_boundaries(self):
        for text, raw in [
            ('迷った あげく', [tok('迷っ', '迷う', ('動詞',)), tok('た', 'た', ('助動詞',)), tok('あげく', 'あげく', ('名詞',))]),
            ('猫のあげく', [tok('猫', '猫', ('名詞',)), tok('の', 'の', ('助詞',)), tok('あげく', 'あげく', ('動詞',))]),
        ]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_ageku(tokens)), [])
