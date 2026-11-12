import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiu.index import RULE as SHORT
from src.grammar_rules.rules.koto_ni_naru.index import RULE as KOTO
from src.grammar_rules.rules.toiukotoninaru.index import RULE, match_toiukotoninaru


class ToiukotoninaruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼が来るということになる。': 'ということになる',
            '明日は雨だということになった。': 'ということになった',
            '彼が来ないと言う事になりました。': 'と言う事になりました',
            '彼が来るっていうことになります。': 'っていうことになります',
            '彼が来るということだ。': None,
            '猫ということになる。': None,
            '彼が来るということにならない。': None,
            '彼が来るということになっている。': None,
            '彼が来るということに なる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiukotoninaru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼が行くということになる。', [
            tok('彼', '彼', ('名詞',)), tok('が', 'が', ('助詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
            tok('と', 'と', ('助詞',)), tok('いう', '言う', ('動詞',)),
            tok('ことになる', 'ことになる', ('名詞', '普通名詞')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, SHORT, KOTO))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'いう', 'ことになる'])
        self.assertEqual(stage.occurrences[SHORT.id].count, 0)
        self.assertEqual(stage.occurrences[KOTO.id].count, 0)
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ということになる')
        self.assertIn(RULE, RULES)

    def test_split_components_yield_to_longer_rule(self):
        source = sentence('彼が行くということになった。', [
            tok('彼', '彼', ('名詞',)), tok('が', 'が', ('助詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
            tok('と', 'と', ('助詞',)), tok('いう', '言う', ('動詞',)),
            tok('こと', 'こと', ('名詞', '普通名詞')), tok('に', 'に', ('助詞',)),
            tok('なっ', 'なる', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, SHORT, KOTO))
        stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[SHORT.id].count, 0)
        self.assertEqual(stage.occurrences[KOTO.id].count, 0)
