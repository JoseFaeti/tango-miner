import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.uede.index import UEDE_EXPRESSION, match_uede
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class UedeTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'よく考えた上で決める。': '上で',
            '話し合ったうえで決める。': 'うえで',
            '日本で暮らす上で必要だ。': '上で',
            '生活の上で大切だ。': '上で',
            '机の上で本を読む。': '上で',  # Literal location is ambiguous.
            '上で待っている。': None,
            '上から見る。': None,
            '考えた 上で決める。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_uede(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_evidence(self):
        source = sentence('よく考えた上で決める。', [
            tok('よく', 'よく', ('副詞',)),
            tok('考え', '考える', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('上', '上', ('名詞',)), tok('で', 'で', ('助詞',)),
            tok('決める。', '決める', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['上', 'で'])
        words = {'考える': object()}
        stage.add_cards(words)
        self.assertIn('考える', words)
        self.assertEqual(words[UEDE_EXPRESSION].frequency, 2)
        self.assertEqual(words[UEDE_EXPRESSION].sentences[0].surface_form, '上で')
