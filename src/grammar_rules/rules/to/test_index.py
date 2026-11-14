import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.to.index import RULE, match_to
from src.grammar_rules.rules.to_omou.index import RULE as TO_OMOU


class ToTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'猫と犬を見る。': 1, '友達と行く。': 1,
                 '彼は来ると思う。': 1, '「行こう」と言った。': 1,
                 '春になると暖かい。': 0, '扉を開けると猫がいた。': 0,
                 '東京に行く。': 0, '猫 と犬': 0}
        for text, count in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_to(tokens)], ['と'] * count)

    def test_reservation_identity_and_overlap(self):
        source = sentence('彼が来ると思います。', [
            tok('彼', '彼', ('代名詞',)), tok('が', 'が', ('助詞', '格助詞')),
            tok('来る', '来る', ('動詞', '一般', '*', '*', 'カ行変格', '終止形-一般')),
            tok('と', 'と', ('助詞', '格助詞')),
            tok('思い', '思う', ('動詞',)), tok('ます', 'ます', ('助動詞',)),
            tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE, TO_OMOU))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[TO_OMOU.id].count, 1)
        self.assertIn('と', [t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')])
        words = {'彼': object(), '来る': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と')
        self.assertIn('彼', words)
        self.assertIn('来る', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
