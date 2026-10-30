import unittest
from unittest.mock import patch

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.ba import RULE, match_ba
from src.grammar_rules.rules.obligation import RULE as OBLIGATION
from tests.test_grammar_te_iru import sentence, tok


class BaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降れば帰る。': True, 'まあ、顔を見れば何が言いたいのかわかるけどな。': True,
            '寒ければ帰る。': True, '行かなければ困る。': True,
            '時間があれば行く。': True, 'どうすればいい。': True,
            '彼がいなければならない。': False, '行かなければいけない。': False,
            '例えば彼が来る。': False, 'そればかりだ。': False,
            '行けば。': False, '行け ば帰る。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ba(aligned)], ['ば'] if expected else [])

    def test_reservation_and_card(self):
        source = sentence('この本を読めば分かるよ。', [
            tok('この本を', '本', ('名詞',)), tok('読め', '読む', ('動詞', '一般', '*', '*', '五段', '仮定形-一般')),
            tok('ば', 'ば', ('助詞', '接続助詞')), tok('分かるよ。', '分かる', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')], ['ば'])
        words = {'読む': object(), '分かる': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ば')
        self.assertIn('読む', words)
        self.assertIn('分かる', words)

    def test_obligation_owns_overlap(self):
        source = sentence('行かなければならない。', [
            tok('行か', '行く', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('なけれ', 'ない', ('助動詞', '*', '*', '*', '助動詞-ナイ', '仮定形-一般')),
            tok('ば', 'ば', ('助詞', '接続助詞')),
            tok('なら', 'なる', ('動詞', '非自立可能', '*', '*', '五段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, OBLIGATION))
        stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[RULE.id].count, 0)
        self.assertEqual(stage.occurrences[OBLIGATION.id].count, 1)

    def test_dictionary_expression_precedence(self):
        source = sentence('この本を読めば分かるよ。', [
            tok('この本を', '本', ('名詞',)), tok('読め', '読む', ('動詞', '一般', '*', '*', '五段', '仮定形-一般')),
            tok('ば', 'ば', ('助詞', '接続助詞')), tok('分かるよ。', '分かる', ('動詞',))])
        merged = [source.tokens[0], dict(surface='読めば', lemma='読めば', pos=('exp',),
                       analysis_rule='jmdict_surface_merge'), source.tokens[-1]]
        stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
        with patch('src.steps.MergeTokensStep.get_jmdict'), \
             patch('src.steps.MergeTokensStep.combine_tokens_jmdict', return_value=merged):
            result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual(stage.occurrences[RULE.id].count, 0)
        self.assertFalse(any(t.get('grammar_reserved') for t in result.data[0].tokens))


if __name__ == '__main__':
    unittest.main()
