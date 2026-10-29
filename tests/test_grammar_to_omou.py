import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.to_omou import RULE, match_to_omou
from src.steps.BuildWordStatsStep import build_word_stats
from src.steps.MergeTokensStep import combine_tokens_jmdict
from unittest.mock import patch
from tests.test_grammar_te_iru import sentence, tok


class ToOmouTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行くと思う。': 'と思う', '雨だと思った。': 'と思った',
                 '行こうと思います。': 'と思います', 'いいと思わない。': 'と思わない',
                 '行くと思っている。': 'と思って', 'そう思う。': None,
                 '友達と思う。': None, '行くと言う。': None, '行くと 思う。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_to_omou(aligned)],
                                 [] if expected is None else [expected])

    def test_grammar_and_lexical_occurrences_are_exclusive(self):
        verb = ('動詞', '一般', '*', '*', '五段', '終止形-一般')
        matched = sentence('明日は行くと思うよ。', [
            tok('明日は', '明日', ('名詞', '普通名詞')),
            tok('行く', '行く', verb), tok('と', 'と', ('助詞', '格助詞')),
            tok('思う', '思う', verb), tok('よ。', 'よ', ('助詞', '*'))])
        lexical = sentence('そう思うこともあるよ。', [
            tok('そう', 'そう', ('副詞', '*')), tok('思う', '思う', verb),
            tok('こともあるよ。', 'こと', ('名詞', '普通名詞'))])
        for source in (matched, lexical):
            for token in source.tokens:
                token.update(base_form=token['lemma'], reading='ヨミ')
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([matched, lexical])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['と', '思う'])
        self.assertFalse(any(t.get('grammar_reserved') for t in data.data[1].tokens))
        class EmptyDictionary:
            def lookup_word(self, word): return []
        merged = [sentence(s.text, combine_tokens_jmdict(s.tokens, EmptyDictionary())) for s in data.data]
        words, _ = build_word_stats(merged)
        stage.add_cards(words)
        self.assertEqual(words['思う'].frequency, 1)
        self.assertEqual(words['行く'].frequency, 1)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と思う')

    def test_dictionary_expression_wins_without_reservation(self):
        source = sentence('行くと思うよ。', [
            tok('行く', '行く', ('動詞', '*', '*', '*', '五段', '終止形-一般')),
            tok('と', 'と', ('助詞', '格助詞')),
            tok('思う', '思う', ('動詞', '一般')), tok('よ。', 'よ', ('助詞', '*'))])
        merged = [dict(surface='行くと思う', lemma='行くと思う', pos=('exp',),
                       analysis_rule='jmdict_surface_merge'), source.tokens[-1]]
        stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
        with patch('src.steps.MergeTokensStep.get_jmdict'), \
             patch('src.steps.MergeTokensStep.combine_tokens_jmdict', return_value=merged):
            data = stage.process(Artifact([source]))
        data = ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual(stage.occurrences[RULE.id].count, 0)
        self.assertFalse(any(t.get('grammar_reserved') for t in data.data[0].tokens))


if __name__ == '__main__':
    unittest.main()
