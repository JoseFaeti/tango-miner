import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.sou_ni_naru.index import RULE, match_sou_ni_naru
from src.grammar_rules.rules.te_iru.index import RULE as TE_IRU
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class SouNiNaruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '転びそうになる。': 'そうになる',
            '転びそうになった。': 'そうになった',
            '泣きそうになりました。': 'そうになりました',
            '倒れそうになっている。': 'そうになって',
            '転びそうにならない。': 'そうにならない',
            '転びそうになりませんでした。': 'そうになりませんでした',
            'そうになる。': None,
            '元気そうになる。': None,
            '転ぶそうになる。': None,
            '転びそうに見える。': None,
            '転びそうに なる。': None,
            '転びそうになっ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_sou_ni_naru(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_connective_overlap(self):
        source = sentence('倒れそうになっている。', [
            tok('倒れ', '倒れる', ('動詞', '一般', '*', '*', '下一段', '連用形-一般')),
            tok('そう', 'そう', ('形状詞', '助動詞語幹')),
            tok('に', 'だ', ('助動詞',)),
            tok('なっ', 'なる', ('動詞', '非自立可能', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('いる', 'いる', ('動詞', '非自立可能', '*', '*', '上一段', '終止形-一般')),
            tok('。', '。', ('補助記号',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE, TE_IRU), prefer_expressions=False)
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[TE_IRU.id].count, 1)
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        self.assertTrue(all(t.get('grammar_reserved') for t in result.data[0].tokens[1:5]))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'そうになって')
