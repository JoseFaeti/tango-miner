import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiu.index import RULE as SHORT
from src.grammar_rules.rules.toiuwakedehanai.index import RULE, match_toiuwakedehanai
from src.grammar_rules.rules.wake_da.index import NEGATIVE


class ToiuwakedehanaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼が来るというわけではない。': 'というわけではない',
            '彼が来たと言う訳ではなかった。': 'と言う訳ではなかった',
            '彼が来るっていうわけじゃない。': 'っていうわけじゃない',
            '彼が来るというわけではありません。': 'というわけではありません',
            '彼が来るというわけではないです。': 'というわけではないです',
            '彼が来るというわけだ。': None,
            '彼が来るというわけはない。': None,
            '猫というわけではない。': None,
            '彼が来ると いうわけではない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiuwakedehanai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼が行くというわけではない。', [
            tok('彼', '彼', ('名詞',)), tok('が', 'が', ('助詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '終止形-一般')),
            tok('と', 'と', ('助詞',)), tok('いう', '言う', ('動詞',)),
            tok('わけ', 'わけ', ('名詞', '普通名詞')), tok('で', 'で', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('ない', 'ない', ('形容詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, SHORT, NEGATIVE))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'いう', 'わけ', 'で', 'は', 'ない'])
        self.assertEqual(stage.occurrences[SHORT.id].count, 0)
        self.assertEqual(stage.occurrences[NEGATIVE.id].count, 0)
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'というわけではない')
        self.assertIn(RULE, RULES)
