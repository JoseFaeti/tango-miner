import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ha.index import RULE, match_ha
from src.grammar_rules.rules.te_wa_ikenai.index import RULE as TE_WA_IKENAI
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class HaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'猫はかわいい。': 1, '今日は雨、明日は晴れ。': 2,
                 '学校には行く。': 1, '読むのは好き。': 1,
                 '「猫」はかわいい。': 1, '花が咲く。': 0,
                 '歯を磨く。': 0, '母は働く。': 1,
                 'はじめまして。': 0, '猫 はかわいい。': 0}
        for text, count in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ha(tokens)], ['は'] * count)

    def test_reservation_identity_and_overlap(self):
        source = sentence('猫は本を読んではいけない。', [
            tok('猫', '猫', ('名詞',)), tok('は', 'は', ('助詞', '係助詞')),
            tok('本', '本', ('名詞',)), tok('を', 'を', ('助詞', '格助詞')),
            tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            tok('で', 'で', ('助詞', '接続助詞')),
            tok('は', 'は', ('助詞', '係助詞')), tok('いけない', 'いけない', ('形容詞',)),
            tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE, TE_WA_IKENAI))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['は', 'で', 'は', 'いけない'])
        self.assertEqual(stage.occurrences[TE_WA_IKENAI.id].count, 1)
        words = {'猫': object(), '本': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'は')
        self.assertIn('猫', words)
        self.assertIn('本', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)


if __name__ == '__main__':
    unittest.main()
