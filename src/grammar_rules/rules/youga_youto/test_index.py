import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.youga_youto.index import RULE, match_youga_youto
from src.grammar_rules.rules.yougamaiga.index import RULE as PAIR_RULE


class YougaYoutoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降ろうが行く。': ['降ろうが'],
            '何を食べようと構わない。': ['食べようと'],
            '誰が来ようが関係ない。': ['来ようが'],
            '何をしようと自由だ。': ['しようと'],
            '王になろうが構わない。': ['なろうが'],
            'まとめ買い忘れるなかれやくそうとキメラのつばさだ。': [],
            '薬草とキメラのつばさだ。': [],
            '薬をやくそうと構わない。': ['やくそうと'],
            '行こうと思う。': [],
            '食べようとする。': [],
            '食べようとした。': [],
            '来ようとしている。': [],
            'しようとしない。': [],
            '日曜と月曜は休みだ。': [],
            'このようとあのようの違いだ。': [],
            '泳ごうと構わない。': ['泳ごうと'],
            '誰が来ようと関係ない。': ['来ようと'],
            '何をしようが自由だ。': ['しようが'],
            '行こうが行くまいが関係ない。': [],
            '降ろうと降るまいと行く。': [],
            '行こう がいい。': [],
            'ようがない。': [],
            'しようがない。': [],
            '仕様が変わった。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_youga_youto(tokens)], expected)

    def test_token_boundaries_and_split_volitional(self):
        cases = [
            ('食べようと', [tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '未然形')),
                         tok('よう', 'よう', ('助動詞',)), tok('と', 'と', ('助詞',))], ['食べようと']),
            ('なろうが', [tok('なろう', 'なる', ('動詞', '一般', '*', '*', '五段', '意志推量形')),
                        tok('が', 'が', ('助詞',))], ['なろうが']),
            ('やくそうと', [tok('やくそう', '薬草', ('名詞',)), tok('と', 'と', ('助詞',))], []),
            ('食べ ようと', [tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '未然形')),
                          tok('よう', 'よう', ('助動詞',)), tok('と', 'と', ('助詞',))], []),
            ('食べよう と', [tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '未然形')),
                          tok('よう', 'よう', ('助動詞',)), tok('と', 'と', ('助詞',))], []),
            ('食べようと', [tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '未然形')),
                         tok('よう', 'よう', ('名詞',)), tok('と', 'と', ('助詞',))], []),
        ]
        for text, raw, expected in cases:
            with self.subTest(text=text, raw=raw):
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_youga_youto(tokens)], expected)

    def test_card_reservation_and_pair_precedence(self):
        source = sentence('雨が降ろうが必ず行く。', [
            tok('雨', '雨', ('名詞',)), tok('が', 'が', ('助詞',)),
            tok('降ろう', '降る', ('動詞', '一般', '*', '*', '五段-ラ行', '意志推量形')),
            tok('が', 'が', ('助詞',)), tok('必ず', '必ず', ('副詞',)), tok('行く', '行く', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, PAIR_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['降ろう', 'が'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '降ろうが')


if __name__ == '__main__':
    unittest.main()
