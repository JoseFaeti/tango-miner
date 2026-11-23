import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.niarazu.index import RULE as NIARAZU_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.zushite.index import RULE, match_zushite


class ZushiteTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '努力せずして成功はない。': True, '戦わずして勝った。': True,
            '食べずして帰った。': True, '来ずして終わった。': True,
            '行かず して帰った。': False, '行か ずして帰った。': False,
            'ずして帰った。': False, '食事をして帰った。': False,
            '戦わずに勝った。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zushite(aligned)],
                                 ['ずして'] if expected else [])

    def test_reservation_and_card_identity(self):
        parts = [tok('戦わ', '戦う', ('動詞', '一般', '*', '*', '五段-ワア行', '未然形-一般')),
                 tok('ず', 'ず', ('助動詞',)), tok('し', 'する', ('動詞',)),
                 tok('て', 'て', ('助詞',)), tok('勝った', '勝つ', ('動詞',)),
                 tok('はずだ', 'はずだ', ('名詞',))]
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('戦わずして勝ったはずだ', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ず', 'し', 'て'])
        words = {'戦う': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ずして')
        self.assertIn('戦う', words)
        self.assertIn('〜ず', words)

    def test_negative_copula_overlap(self):
        parts = [tok('真実', '真実', ('名詞',)), tok('に', 'に', ('助詞',)),
                 tok('あら', 'ある', ('動詞', '非自立可能', '*', '*', '五段-ラ行', '未然形-一般')),
                 tok('ず', 'ず', ('助動詞',)), tok('し', 'する', ('動詞',)),
                 tok('て', 'て', ('助詞',)), tok('虚偽だった', '虚偽', ('名詞',))]
        stage = GrammarStep(rules=(RULE, NIARAZU_RULE))
        stage.process(Artifact([sentence('真実にあらずして虚偽だった', parts)]))
        words = {}
        stage.add_cards(words)
        self.assertIn(RULE.expression, words)
        self.assertIn(NIARAZU_RULE.expression, words)
