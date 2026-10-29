import unittest
from unittest.mock import patch

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.te_iku import RULE, match_te_iku
from tests.test_grammar_te_iru import sentence, tok


class TeIkuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'雨が降っていく。': 'ていく', '変わっていった。': 'ていった',
                 '歩いて行く。': 'て行く', '歩いていきます。': 'ていきます',
                 '歩いていきませんでした。': 'ていきませんでした',
                 '歩いていかない。': 'ていかない', '歩いていかなかった。': 'ていかなかった',
                 '歩いていって。': 'ていって', '歩いていこう。': 'ていこう',
                 '家に行く。': None, '食べてく。': None, '持っていく。': None,
                 '雨が降ってきた。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_iku(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_expression_precedence(self):
        source = sentence('景色が変わっていったよ。', [
            tok('景色が', '景色', ('名詞',)),
            tok('変わっ', '変わる', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('いっ', 'いく', ('動詞', '非自立可能')),
            tok('た', 'た', ('助動詞',)), tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = Artifact([source])
        stage.process(data)
        reserved = ExcludeGrammarTokensStep(stage).process(data).data[0].tokens
        self.assertEqual([t['surface'] for t in reserved if t.get('grammar_reserved')], ['て', 'いっ', 'た'])
        words = {'変わる': object(), 'いく': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ていった')
        self.assertIn('変わる', words)
        self.assertIn('いく', words)
        merged = [source.tokens[0], {'surface': '変わっていった', 'lemma': '変わる',
                  'pos': ('exp',), 'analysis_rule': 'jmdict_surface_merge'}, source.tokens[-1]]
        stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
        with patch('src.steps.MergeTokensStep.get_jmdict'), patch('src.steps.MergeTokensStep.combine_tokens_jmdict', return_value=merged):
            stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[RULE.id].count, 0)

    def test_incomplete_and_gapped_forms(self):
        tokens = [dict(surface='歩い', lemma='歩く', pos=['動詞', '*', '*', '*', '*', '連用形'], span=[0, 2]),
                  dict(surface='て', lemma='て', pos=['助詞', '接続助詞'], span=[2, 3]),
                  dict(surface='いっ', lemma='いく', pos=['動詞', '非自立可能'], span=[3, 5])]
        self.assertEqual(list(match_te_iku(tokens)), [])
        tokens[-1]['surface'] = 'いく'
        tokens[-1]['span'] = [4, 6]
        self.assertEqual(list(match_te_iku(tokens)), [])


if __name__ == '__main__':
    unittest.main()
