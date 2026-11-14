import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tsutsu.index import TSUTSU_EXPRESSION, match_tsutsu
from src.grammar_rules.rules.tsutsuaru.index import TSUTSUARU_EXPRESSION


class TsutsuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '音楽を聴きつつ歩く。': True,
            '悪いと知りつつ続ける。': True,
            '悪いと知りつつも続ける。': False,
            '準備されつつ進む。': True,
            '人口が増えつつある。': False,
            '人口が増えつつありました。': False,
            '筒を持って歩く。': False,
            '読みつつ。次の日に話す。': False,
            '読み つつ考える。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tsutsu(aligned)],
                                 ['つつ'] if expected else [])

    def test_card_reservation_and_progressive_precedence(self):
        source = sentence('本を読みつつ考える。', [
            tok('本を', '本', ('名詞',)),
            tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('つつ', 'つつ', ('助詞', '接続助詞')),
            tok('考える', '考える', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        progressive = sentence('人口が増えつつある。', [
            tok('人口が', '人口', ('名詞',)),
            tok('増え', '増える', ('動詞', '一般', '*', '*', '下一段', '連用形-一般')),
            tok('つつ', 'つつ', ('助詞', '接続助詞')),
            tok('ある', 'ある', ('動詞', '非自立可能', '*', '*', '五段', '終止形-一般')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = Artifact([source, source, progressive, progressive])
        stage.process(artifact)
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[TSUTSU_EXPRESSION].frequency, 2)
        self.assertEqual(words[TSUTSU_EXPRESSION].sentences[0].surface_form, 'つつ')
        self.assertEqual(len(words[TSUTSU_EXPRESSION].sentences), 1)
        self.assertEqual(words[TSUTSUARU_EXPRESSION].frequency, 2)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
        self.assertTrue(artifact.data[0].tokens[2].get('grammar_reserved'))
