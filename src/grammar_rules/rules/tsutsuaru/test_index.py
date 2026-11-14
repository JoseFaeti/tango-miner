import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tsutsuaru.index import TSUTSUARU_EXPRESSION, match_tsutsuaru


class TsutsuaruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '状況は変化しつつある。': 'つつある',
            '人口が増えつつあります。': 'つつあります',
            '文化が失われつつあった。': 'つつあった',
            '変わりつつあっても続く。': 'つつあって',
            '読みつつ考える。': None,
            '筒がある。': None,
            '変わりつつもある。': None,
            '変わりつつ あった。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tsutsuaru(aligned)],
                                 [expected] if expected else [])

    def test_reservation_and_card_keep_verb(self):
        source = sentence('人口が増えつつある。', [
            tok('人口が', '人口', ('名詞',)),
            tok('増え', '増える', ('動詞', '一般', '*', '*', '下一段', '連用形-一般')),
            tok('つつ', 'つつ', ('助詞', '接続助詞')),
            tok('ある', 'ある', ('動詞', '非自立可能')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = Artifact([source, source])
        stage.process(artifact)
        words = {'増える': object()}
        stage.add_cards(words)
        self.assertIn('増える', words)
        self.assertEqual(words[TSUTSUARU_EXPRESSION].frequency, 2)
        self.assertEqual(words[TSUTSUARU_EXPRESSION].sentences[0].surface_form, 'つつある')
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
        self.assertTrue(artifact.data[0].tokens[2].get('grammar_reserved'))
        self.assertTrue(artifact.data[0].tokens[3].get('grammar_reserved'))
