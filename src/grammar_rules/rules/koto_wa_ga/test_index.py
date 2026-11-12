import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.koto_wa_ga.index import RULE, match_koto_wa_ga
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KotoWaGaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '食べることは食べるが、少ない。': True,
            '高いことは高いけど、便利だ。': True,
            '行ったことは行ったが、会えなかった。': True,
            '静かなことは静かだが、狭い。': True,
            '食べることは好きだが、料理は嫌い。': False,
            '食べることは食べる。': False,
            '食べることは食べるが。次へ行く。': False,
            '食べることはない。': False,
            '食べることは食べるが猫がいる。': True,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_koto_wa_ga(aligned)],
                                 ['ことは'] if expected else [])

    def test_reservation_and_identity(self):
        source = sentence('食べることは食べるが少ない。', [
            tok('食べる', '食べる', ('動詞',)), tok('こと', 'こと', ('名詞',)),
            tok('は', 'は', ('助詞',)), tok('食べる', '食べる', ('動詞',)),
            tok('が', 'が', ('助詞', '接続助詞')), tok('少ない', '少ない', ('形容詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['こと', 'は'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        from src.GrammarConstants import GRAMMAR_EXPRESSIONS
        from src.grammar_rules.registry import RULES
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)
