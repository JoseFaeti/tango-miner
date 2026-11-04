import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_mo.index import RULE, match_te_mo
from src.grammar_rules.rules.te_mo_ii.index import RULE as PERMISSION
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TeMoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降っても行く。': ['ても'], '読んでも分からない。': ['でも'],
            '高くても買う。': ['ても'], '食べなくても大丈夫。': ['ても'],
            '行っても、いい結果にならない。': ['ても'],
            '行っても行かなくても同じ。': ['ても', 'ても'],
            '行ってもいい。': [], '読んでもよかった。': [],
            '誰でも知っている。': [], '静かでも眠れない。': [],
            '行っても。': [], '行って も困る。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_mo(aligned)], expected)

    def test_card_reservation_and_identity(self):
        verb = ('動詞', '一般', '*', '*', '五段', '連用形-促音便')
        source = sentence('明日は雨が降っても学校に行く。', [tok('明日は雨が', '雨', ('名詞',)),
            tok('降っ', '降る', verb), tok('て', 'て', ('助詞', '接続助詞')),
            tok('も', 'も', ('助詞', '係助詞')), tok('学校に', '学校', ('名詞',)), tok('行く', '行く', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, PERMISSION))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')], ['て', 'も'])
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        words = {}
        stage.add_cards(words)
        self.assertIn(RULE.expression, words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)


if __name__ == '__main__':
    unittest.main()
