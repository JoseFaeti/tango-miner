import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.koto_wa_nai.index import RULE, match_koto_wa_nai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KotoWaNaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'心配することはない。': 'ことはない',
                 '決して嘘をつくことはない。': 'ことはない',
                 '行くことはなかった。': 'ことはなかった',
                 '行くことはありません。': 'ことはありません',
                 '行くことはありませんでした。': 'ことはありませんでした',
                 '行ったことはない。': None, '行かないことはない。': None,
                 '大事なことはない。': None, '行くことがない。': None,
                 '行くことはある。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_koto_wa_nai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('心配することはない。', [
            tok('心配', '心配', ('名詞',)),
            tok('する', 'する', ('動詞', '非自立可能', '*', '*', 'サ行変格', '連体形-一般')),
            tok('こと', 'こと', ('名詞',)), tok('は', 'は', ('助詞',)),
            tok('ない', 'ない', ('形容詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['こと', 'は', 'ない'])
        words = {'する': object()}
        stage.add_cards(words)
        self.assertIn('する', words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        from src.GrammarConstants import GRAMMAR_EXPRESSIONS
        from src.grammar_rules.registry import RULES
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)
