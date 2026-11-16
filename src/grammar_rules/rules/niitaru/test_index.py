import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.niitaru.index import RULE, match_niitaru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiitaruTests(unittest.TestCase):
    def test_sudachi_forms(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'結論に至る。': 'に至る', '決断に至った。': 'に至った',
                 '結論に至ります。': 'に至ります', '結論に至って気づいた。': None,
                 '大人に至るまで楽しめる。': None, '駅へ至る。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niitaru(tokens)],
                                 [] if expected is None else [expected])

    def test_forms_and_controls(self):
        def check(parts, expected):
            raw = [tok(*part) for part in parts]
            text = ''.join(t['surface'] for t in raw)
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_niitaru(tokens)], expected)
        noun = ('結論', '結論', ('名詞',))
        ni = ('に', 'に', ('助詞',))
        verb = lambda s: (s, '至る', ('動詞',))
        for tail, expected in [([verb('至る')], ['に至る']),
                               ([verb('至っ'), ('た', 'た', ('助動詞',))], ['に至った']),
                               ([verb('至り'), ('ます', 'ます', ('助動詞',))], ['に至ります']),
                               ([verb('至る'), ('まで', 'まで', ('助詞',))], []),
                               ([verb('至っ'), ('て', 'て', ('助詞',))], []),
                               ([verb('至っ')], [])]:
            with self.subTest(tail=tail):
                check([noun, ni] + tail, expected)
        check([noun, ('へ', 'へ', ('助詞',)), verb('至る')], [])
        check([noun, ni, ('至る', '至る', ('名詞',))], [])
        check([('決断する', 'する', ('動詞',)), ni, verb('至る')], ['に至る'])

    def test_reservation_and_identity(self):
        source = sentence('私たちは結論に至った。', [tok('私たちは', '私', ('名詞',)), tok('結論', '結論', ('名詞',)),
                          tok('に', 'に', ('助詞',)), tok('至っ', '至る', ('動詞',)),
                          tok('た', 'た', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '至っ', 'た'])
        words = {'結論': object()}
        stage.add_cards(words)
        self.assertIn('結論', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に至った')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
