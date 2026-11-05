import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.you_ni_naru.index import RULE, match_you_ni_naru
from src.grammar_rules.rules.you_ni.index import RULE as YOU_NI
from src.grammar_rules.rules.te_iru.index import RULE as TE_IRU
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class YouNiNaruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '日本語が話せるようになった。': 'ようになった',
            '毎日走るようになる。': 'ようになる',
            '早く起きないようになりました。': 'ようになりました',
            '鳥のようになる。': None,
            '静かなようになる。': None,
            '泳げるように練習する。': None,
            '泳げるように なる。': None,
            '泳げるようになっている。': 'ようになって',
            '毎日走るようになって、元気になった。': 'ようになって',
            '鳥のようになっている。': None,
            '泳げるようになっ ている。': None,
            '泳げるようになっ。': None,
            '泳げるようにならない。': 'ようにならない',
            '泳げるようにならなかった。': 'ようにならなかった',
            '泳げるようになりません。': 'ようになりません',
            '泳げるようになりませんでした。': 'ようになりませんでした',
            '鳥のようにならない。': None,
            '泳げるようになら ない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_you_ni_naru(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_overlap(self):
        source = sentence('日本語が話せるようになったよ。', [
            tok('日本語が', '日本語', ('名詞',)),
            tok('話せる', '話せる', ('動詞', '一般', '*', '*', '下一段', '連体形-一般')),
            tok('よう', 'よう', ('形状詞',)),
            tok('に', 'だ', ('助動詞',)),
            tok('なっ', 'なる', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('よ。', 'よ', ('助詞',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE, YOU_NI))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[YOU_NI.id].count, 0)
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['よう', 'に', 'なっ', 'た'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ようになった')

    def test_connective_overlap_with_te_iru(self):
        source = sentence('泳げるようになっている。', [
            tok('泳げる', '泳げる', ('動詞', '一般', '*', '*', '下一段', '連体形-一般')),
            tok('よう', 'よう', ('形状詞',)), tok('に', 'だ', ('助動詞',)),
            tok('なっ', 'なる', ('動詞', '非自立可能', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('いる', 'いる', ('動詞', '非自立可能', '*', '*', '上一段', '終止形-一般')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, YOU_NI, TE_IRU), prefer_expressions=False)
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[YOU_NI.id].count, 0)
        self.assertEqual(stage.occurrences[TE_IRU.id].count, 1)
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        self.assertTrue(all(t.get('grammar_reserved') for t in result.data[0].tokens[1:6]))
