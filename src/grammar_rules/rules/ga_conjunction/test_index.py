import copy
import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ga.index import RULE as SUBJECT_GA
from src.grammar_rules.rules.ga_conjunction.index import RULE, match_ga_conjunction
from src.grammar_rules.rules.ndakedo_ndesuga.index import RULE as EXPLANATORY
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class GaConjunctionTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'雨が降るが、行く。': 1, '高いが、買う。': 1,
                 '学生ですが、働く。': 1, '行ったが、帰った。': 1,
                 '行かないが、見る。': 1, 'すみませんが。': 1,
                 '猫がいる。': 0, '蛾を見る。': 0, 'がんばる。': 0,
                 '降る が行く。': 0}
        for text, count in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ga_conjunction(tokens)], ['が'] * count, raw)

    def test_identity_reservation_and_overlap(self):
        source = sentence('猫が読むんですが今日は休みます。', [
            tok('猫', '猫', ('名詞',)), tok('が', 'が', ('助詞', '格助詞')),
            tok('読む', '読む', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('ん', 'ん', ('助詞', '準体助詞')),
            tok('です', 'です', ('助動詞', '*', '*', '*', '助動詞-デス', '終止形-一般')),
            tok('が', 'が', ('助詞', '接続助詞')),
            tok('今日は休みます。', '今日', ('名詞',))])
        stage = GrammarStep(rules=(SUBJECT_GA, EXPLANATORY, RULE))
        stage.process(Artifact([copy.deepcopy(source)]))
        self.assertEqual(stage.occurrences[SUBJECT_GA.id].count, 1)
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[EXPLANATORY.id].count, 1)
        single = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(single).process(single.process(Artifact([source])))
        self.assertIn('が', [token['surface'] for token in data.data[0].tokens
                            if token.get('grammar_reserved')])
        words = {'猫': object(), '読む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'が')
        self.assertIn('読む', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertNotEqual(RULE.expression, SUBJECT_GA.expression)
