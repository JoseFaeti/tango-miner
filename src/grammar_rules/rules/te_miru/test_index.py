import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_miru.index import RULE, match_te_miru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TeMiruTests(unittest.TestCase):
    def test_incomplete_conditional_and_nonadjacent_suffix_do_not_match(self):
        text = '読んでみれ ば'
        raw = [tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
               tok('で', 'で', ('助詞', '接続助詞')),
               tok('みれ', 'みる', ('動詞', '非自立可能', '*', '*', '上一段', '仮定形-一般')),
               tok('ば', 'ば', ('助詞', '接続助詞'))]
        aligned, error = aligned_tokens(text, raw)
        self.assertIsNone(error)
        self.assertEqual(list(match_te_miru(aligned)), [])
        self.assertEqual(list(match_te_miru(aligned[:-1])), [])

    def test_analyzer_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'食べてみる。': 'てみる', 'やってみた。': 'てみた',
                 '読んでみます。': 'でみます', '試してみない。': 'てみない',
                 '読んでみて。': 'でみて', '読んでみれば。': 'でみれば',
                 '読んでみよう。': 'でみよう', '本を見れば。': None,
                 '本を見てみる。': 'てみる', '本を見ている。': None,
                 '本を見て。': None, '雨で見る。': None,
                 '食べて 見る。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_miru(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_keeps_verb_and_literal_control(self):
        verb = ('動詞', '一般', '*', '*', '下一段', '連用形-一般')
        matched = sentence('今日は食べてみたので、試した。', [tok('今日は', '今日', ('名詞',)), tok('食べ', '食べる', verb),
                         tok('て', 'て', ('助詞', '接続助詞')),
                         tok('み', 'みる', ('動詞', '非自立可能', '*', '*', '上一段', '連用形-一般')),
                         tok('た', 'た', ('助動詞',)), tok('ので、', 'ので', ('助詞',)), tok('試し', '試す', verb), tok('た', 'た', ('助動詞',)), tok('。', '。', ('補助記号',))])
        control = sentence('今日は本で見る。', [tok('今日は本', '本', ('名詞', '普通名詞')),
                         tok('で', 'で', ('助詞', '格助詞')),
                         tok('見る', '見る', ('動詞', '一般', '*', '*', '上一段', '連体形-一般')),
                         tok('今日は', '今日', ('名詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([matched, control])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['て', 'み', 'た'])
        self.assertFalse(any(t.get('grammar_reserved') for t in result.data[1].tokens))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'てみた')


if __name__ == '__main__':
    unittest.main()
