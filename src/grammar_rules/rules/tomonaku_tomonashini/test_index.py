import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tomonaku_tomonashini.index import RULE, match_tomonaku_tomonashini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TomonakuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '見るともなく眺める。': 'ともなく',
            '言うともなく言った。': 'ともなく',
            '聞くともなしに聞いた。': 'ともなしに',
            '友なく暮らす。': None, '何とはなしに思う。': None,
            '見ようともなくなる。': None, '見る ともなく眺める。': None,
            '見るとも なく眺める。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tomonaku_tomonashini(aligned)],
                                 [expected] if expected else [])

    def test_reservation_and_card_identity(self):
        parts = [tok('聞く', '聞く', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
                 tok('とも', 'とも', ('助詞',)), tok('なし', 'なし', ('名詞',)),
                 tok('に', 'に', ('助詞',)), tok('聞いた', '聞く', ('動詞',))]
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('聞くともなしに聞いた', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['とも', 'なし', 'に'])
        words = {'聞く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ともなしに')
        self.assertIn('聞く', words)
