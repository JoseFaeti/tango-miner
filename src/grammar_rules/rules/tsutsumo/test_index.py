import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tsutsu.index import TSUTSU_EXPRESSION
from src.grammar_rules.rules.tsutsumo.index import TSUTSUMO_EXPRESSION, match_tsutsumo


class TsutsumoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '悪いと知りつつも続ける。': True,
            '迷いつつも決めた。': True,
            '批判されつつも続けた。': True,
            '音楽を聴きつつ歩く。': False,
            '人口が増えつつある。': False,
            '筒も持って歩く。': False,
            '知りつつも。次に進む。': False,
            '知りつつ も続ける。': False,
            '知り つつも続ける。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tsutsumo(aligned)],
                                 ['つつも'] if expected else [])

    def test_card_identity_and_reservation(self):
        source = sentence('悪いと知りつつも続ける。', [
            tok('悪いと', '悪い', ('形容詞',)),
            tok('知り', '知る', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('つつ', 'つつ', ('助詞', '接続助詞')),
            tok('も', 'も', ('助詞', '係助詞')),
            tok('続ける', '続ける', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = Artifact([source, source])
        stage.process(artifact)
        words = {'知る': object()}
        stage.add_cards(words)
        self.assertIn('知る', words)
        self.assertNotIn(TSUTSU_EXPRESSION, words)
        self.assertEqual(words[TSUTSUMO_EXPRESSION].frequency, 2)
        self.assertEqual(words[TSUTSUMO_EXPRESSION].sentences[0].surface_form, 'つつも')
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
        self.assertTrue(artifact.data[0].tokens[2].get('grammar_reserved'))
        self.assertTrue(artifact.data[0].tokens[3].get('grammar_reserved'))
