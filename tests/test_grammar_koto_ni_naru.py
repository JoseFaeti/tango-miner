import unittest
from unittest.mock import patch

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.koto_ni_naru import RULE, match_koto_ni_naru
from tests.test_grammar_te_iru import sentence, tok


class KotoNiNaruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '迷惑かけることになるくらいなら俺に…': 'ことになる',
            '私たち２人して路頭に迷うことになるんだよ': 'ことになる',
            '来年行くことになった。': 'ことになった',
            '行かないことになりました。': 'ことになりました',
            'そんなことになる。': None,
            '先のことになるし。': None,
            '困ったことになる。': None,
            '行くことになっている。': None,
            '行くことにならない。': None,
            '行くことに なる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_koto_ni_naru(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_noun_control(self):
        verb = ('動詞', '一般', '*', '*', '五段', '終止形-一般')
        matched = sentence('明日は遠くへ行くことになるよ。', [
            tok('明日は遠くへ', '明日', ('名詞',)), tok('行く', '行く', verb),
            tok('ことになる', 'ことになる', ('名詞', '普通名詞')),
            tok('よ。', 'よ', ('助詞',))])
        lexical = sentence('そんなことになるとは思わないよ。', [
            tok('そんな', 'そんな', ('形状詞', '一般')),
            tok('ことになる', 'ことになる', ('名詞', '普通名詞')),
            tok('とは思わないよ。', 'とは', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([matched, lexical])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ことになる'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        self.assertFalse(any(t.get('grammar_reserved') for t in result.data[1].tokens))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ことになる')

    def test_complete_dictionary_expression_takes_precedence(self):
        source = sentence('明日は行くことになるよ。', [
            tok('明日は', '明日', ('名詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
            tok('ことになる', 'ことになる', ('名詞', '普通名詞')),
            tok('よ。', 'よ', ('助詞',))])
        merged = [source.tokens[0], dict(surface='行くことになる', lemma='行くことになる',
                  pos=('exp',), analysis_rule='jmdict_surface_merge'), source.tokens[-1]]
        stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
        with patch('src.steps.MergeTokensStep.get_jmdict'), \
             patch('src.steps.MergeTokensStep.combine_tokens_jmdict', return_value=merged):
            result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual(stage.occurrences[RULE.id].count, 0)
        self.assertFalse(any(t.get('grammar_reserved') for t in result.data[0].tokens))


if __name__ == '__main__':
    unittest.main()
