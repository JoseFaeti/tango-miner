import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ga.index import RULE, match_ga
from src.grammar_rules.rules.koto_ga_dekiru.index import RULE as KOTO_GA_DEKIRU
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class GaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'猫がいる。': 1, '雨が降り、風が吹く。': 2,
                 '猫が好き。': 1, '読むのが好き。': 1,
                 '「猫」がいる。': 1, '雨が降るが、行く。': 1,
                 '学校へ行く。': 0, '蛾を見る。': 0,
                 'がんばる。': 0, '猫 がいる。': 0}
        for text, count in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ga(tokens)], ['が'] * count)

    def test_reservation_identity_and_overlap(self):
        source = sentence('猫が読むことができる。', [
            tok('猫', '猫', ('名詞',)), tok('が', 'が', ('助詞', '格助詞')),
            tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('こと', 'こと', ('名詞', '普通名詞', '一般')),
            tok('が', 'が', ('助詞', '格助詞')),
            tok('できる', 'できる', ('動詞', '一般', '*', '*', '下一段', '終止形-一般')),
            tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE, KOTO_GA_DEKIRU))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['が', 'こと', 'が', 'できる'])
        self.assertEqual(stage.occurrences[RULE.id].count, 2)
        self.assertEqual(stage.occurrences[KOTO_GA_DEKIRU.id].count, 1)
        words = {'猫': object(), '読む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'が')
        self.assertIn('猫', words)
        self.assertIn('読む', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)


if __name__ == '__main__':
    unittest.main()
