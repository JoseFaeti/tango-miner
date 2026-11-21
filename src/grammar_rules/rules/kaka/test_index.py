import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kaka.index import RULE, match_kaka
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KakaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行くか行かないか決める。': True,
            '雨か雪か分からない。': True,
            '高いか安いか調べる。': True,
            '行くか、帰るか決める。': True,
            '行くかどうか分からない。': False,
            '行くか。帰るか。': False,
            '彼のかばん。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual(bool(list(match_kaka(tokens))), expected, raw)

    def test_reservation_and_identity(self):
        source = sentence('雨か雪か分からない。', [tok('雨', '雨', ('名詞',)),
                          tok('か', 'か', ('助詞',)), tok('雪', '雪', ('名詞',)),
                          tok('か', 'か', ('助詞',)), tok('分からない', '分かる', ('動詞',)),
                          tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['か'])
        words = {'雨': object(), '雪': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'か')
        self.assertIn(RULE, RULES)
