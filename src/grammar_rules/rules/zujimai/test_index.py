import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.zu.index import RULE as SHORT_RULE
from src.grammar_rules.rules.zujimai.index import RULE, match_zujimai


class ZujimaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行かずじまいだった。': 'ずじまい', '食べずじまいです。': 'ずじまい',
            'せずじまいになった。': 'ずじまい', '来ずじまいで終わった。': 'ずじまい',
            '行かず仕舞い。': 'ず仕舞い', '行かずじまいの旅。': 'ずじまい',
            '水じまい。': None, '行かずに帰る。': None,
            'ずじまい。': None, '行かず じまい。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zujimai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_overlapping_cards(self):
        parts = [tok('行か', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '未然形-一般')),
                 tok('ず', 'ず', ('助動詞',)), tok('じまい', 'じまい', ('接尾辞',)),
                 tok('だった。', 'だ', ('助動詞',))]
        stage = GrammarStep(rules=(SHORT_RULE, RULE))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('行かずじまいだった。', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ず', 'じまい'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[SHORT_RULE.expression].sentences[0].surface_form, 'ず')
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ずじまい')
