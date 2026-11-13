import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kara_node.index import RULE as CAUSAL_RULE
from src.grammar_rules.rules.karaniha.index import RULE, match_karaniha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KaranihaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '引き受けたからには最後まで頑張る。': True,
            '行くからには楽しもう。': True,
            '学生だからには勉強しよう。': True,
            '高いからには品質がいいはずだ。': True,
            '駅からには電車が出る。': False,
            '食べてからには出かけよう。': False,
            '行くから に は頑張ろう。': False,
            '行くから頑張ろう。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_karaniha(aligned)],
                                 ['からには'] if expected else [])

    def test_cards_and_reservation(self):
        source = sentence('行くからには頑張る。', [
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '終止形-一般')),
            tok('から', 'から', ('助詞', '接続助詞')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('は', 'は', ('助詞', '係助詞')),
            tok('頑張る。', '頑張る', ('動詞',))])
        stage = GrammarStep(rules=(RULE, CAUSAL_RULE))
        artifact = stage.process(Artifact([source]))
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'からには')
        self.assertIn(CAUSAL_RULE.expression, words)
        self.assertIn('行く', words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['から', 'に', 'は'])
