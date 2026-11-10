import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import NAOSU_GRAMMAR_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.naosu.index import match_naosu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NaosuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読み直す。': '読み直す', '書き直した。': '書き直した',
            'やり直します。': 'やり直します', '見直さなかった。': '見直さなかった',
            '読みなおして。': '読みなおして', '読み直せば。': '読み直せば',
            '時計を直す。': None, '立ち直る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naosu(aligned)],
                                 [] if expected is None else [expected])

    def test_split_tokens_and_incomplete_forms(self):
        verb = tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        suffix = tok('直し', '直す', ('動詞', '非自立可能'))
        past = tok('た', 'た', ('助動詞',))
        for raw, expected in [([verb, suffix, past], ['読み直した']),
                              ([verb, suffix], []),
                              ([tok('本', '本', ('名詞',)), suffix, past], [])]:
            text = ''.join(t['surface'] for t in raw)
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_naosu(aligned)], expected)

    def test_card_keeps_lexical_candidate_and_source(self):
        source = sentence('私は本を読み直すと思います。', [tok('私は本を', '本', ('名詞',)),
                          tok('読み直す', '読み直す', ('動詞',)), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読み直す': object()}
        stage.add_cards(words)
        self.assertIn('読み直す', words)
        self.assertEqual(words[NAOSU_GRAMMAR_EXPRESSION].frequency, 2)
        self.assertEqual(words[NAOSU_GRAMMAR_EXPRESSION].sentences[0].surface_form, '読み直す')
        artifact = ExcludeGrammarTokensStep(stage).process(Artifact([source, source]))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))


if __name__ == '__main__':
    unittest.main()
