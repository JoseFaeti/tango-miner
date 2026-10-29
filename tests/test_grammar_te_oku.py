import unittest
from pathlib import Path
from unittest.mock import patch

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.te_oku import RULE, match_te_oku
from src.grammar_rules.rules.tara import RULE as TARA_RULE
from tests.test_grammar_te_iru import sentence, tok


class TeOkuTests(unittest.TestCase):
    def test_sudachi_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読んでおく。': 'でおく', '買っておいた。': 'ておいた',
            '買っておいて。': 'ておいて', '準備しておきます。': 'ておきます',
            '開けておきませんでした。': 'ておきませんでした',
            '買っておかない。': 'ておかない', '買っておかなかった。': 'ておかなかった',
            '買っておこう。': 'ておこう', '買っておけば。': 'ておけば',
            '買っておいたらいい。': 'ておいたら', 'ほっといたらどっか行くよ。': 'といたら',
            '置いとかなきゃ。': 'とかなきゃ', '置いておかなくていい。': 'ておかなく',
            '買っとく。': 'とく', '買っといた。': 'といた',
            '読んどく。': 'どく', '出しといて。': 'といて',
            '机に置いておく。': 'ておく', '机に本を置く。': None,
            '手を洗ってから寝る。': None, '読んどいた。': None,
            '読んでおき。': None, '忘れないでおいて。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_oku(aligned)],
                                 [] if expected is None else [expected])

    def test_incomplete_gapped_and_misleading_tokens(self):
        verb = dict(surface='買っ', lemma='買う', pos=['動詞', '一般', '*', '*', '*', '連用形'], span=[0, 2])
        te = dict(surface='て', lemma='て', pos=['助詞', '接続助詞'], span=[2, 3])
        oku = dict(surface='おい', lemma='おく', pos=['動詞', '非自立可能'], span=[3, 5])
        self.assertEqual(list(match_te_oku([verb, te, oku])), [])
        oku['surface'] = 'おく'
        oku['span'] = [4, 6]
        self.assertEqual(list(match_te_oku([verb, te, oku])), [])
        oku['span'] = [3, 5]
        te['pos'] = ['助詞', '格助詞']
        self.assertEqual(list(match_te_oku([verb, te, oku])), [])
        te['pos'] = ['助詞', '接続助詞']
        oku['pos'] = ['名詞', '普通名詞']
        self.assertEqual(list(match_te_oku([verb, te, oku])), [])

    def test_card_reservation_and_expression_precedence(self):
        source = sentence('明日までに買っておいたよ。', [
            tok('明日までに', '明日', ('名詞',)),
            tok('買っ', '買う', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('おい', 'おく', ('動詞', '非自立可能')),
            tok('た', 'た', ('助動詞',)), tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = Artifact([source])
        stage.process(data)
        reserved = ExcludeGrammarTokensStep(stage).process(data).data[0].tokens
        self.assertEqual([t['surface'] for t in reserved if t.get('grammar_reserved')], ['て', 'おい', 'た'])
        self.assertFalse(reserved[1].get('grammar_reserved'))
        words = {'買う': object(), 'おく': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ておいた')
        self.assertIn('買う', words)
        self.assertIn('おく', words)
        merged = [source.tokens[0], {'surface': '買っておいた', 'lemma': '買う',
                  'pos': ('exp',), 'analysis_rule': 'jmdict_surface_merge'}, source.tokens[-1]]
        stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
        with patch('src.steps.MergeTokensStep.get_jmdict'), patch('src.steps.MergeTokensStep.combine_tokens_jmdict', return_value=merged):
            stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[RULE.id].count, 0)

    def test_conditional_overlap_and_ineligible_example(self):
        source = sentence('ほっといたらどっか行くよ。', [
            tok('ほっ', 'ほる', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('とい', 'とく', ('助動詞', '*', '*', '*', '五段', '連用形-イ音便')),
            tok('たら', 'た', ('助動詞', '*', '*', '*', '助動詞-タ', '仮定形-一般')),
            tok('どっか行くよ。', 'どっか', ('名詞',))])
        stage = GrammarStep(rules=(RULE, TARA_RULE))
        data = stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences[TARA_RULE.id].count, 1)
        reserved = ExcludeGrammarTokensStep(stage).process(data).data[0].tokens
        self.assertEqual([t['surface'] for t in reserved if t.get('grammar_reserved')], ['とい', 'たら'])
        self.assertFalse(reserved[0].get('grammar_reserved'))
        short = sentence('買っとく', [tok('買っ', '買う', ('動詞', '一般', '*', '*', '五段', '連用形')),
                                    tok('とく', 'とく', ('助動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([short]))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertFalse(stage.occurrences[RULE.id].examples)
        self.assertFalse(any(t.get('grammar_reserved') for t in ExcludeGrammarTokensStep(stage).process(data).data[0].tokens))

    def test_overlap_requires_a_longer_dictionary_expression(self):
        source = sentence('早めに買っておけばいい。', [
            tok('早めに', '早め', ('名詞',)),
            tok('買っ', '買う', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('おけ', 'おく', ('動詞', '非自立可能')),
            tok('ば', 'ば', ('助詞', '接続助詞')),
            tok('いい', 'いい', ('形容詞', '非自立可能')), tok('。', '。', ('補助記号',))])
        overlap = {'surface': 'おけばいい', 'lemma': 'おけばいい', 'pos': ('exp',),
                   'analysis_rule': 'jmdict_surface_merge', 'dictionary_expression': False}
        merged = source.tokens[:3] + [overlap, source.tokens[-1]]
        for is_expression, expected in ((False, 1), (True, 0)):
            with self.subTest(is_expression=is_expression):
                overlap['dictionary_expression'] = is_expression
                stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
                with patch('src.steps.MergeTokensStep.get_jmdict'), patch('src.steps.MergeTokensStep.combine_tokens_jmdict', return_value=merged):
                    stage.process(Artifact([source]))
                self.assertEqual(stage.occurrences[RULE.id].count, expected)

    def test_real_merger_and_lexical_component_evidence(self):
        from src.TokenizedSentence import TokenizedSentence
        from src.steps.TokenizeStep import tokenize_raw_sentences
        from src.steps.MergeTokensStep import combine_tokens_jmdict, get_jmdict
        from src.steps.BuildWordStatsStep import build_word_stats

        try:
            dictionary = get_jmdict()
            texts = ['これで１～２週間ほっとく', '今は本を買っておくよ', '明日は机に本を置くよ']
            raw = tokenize_raw_sentences(texts, tag='source', source_path=Path('source.txt'))
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi or JMdict unavailable: {exc}')

        baseline = [TokenizedSentence(s.text, s.tag, s.origin, combine_tokens_jmdict(s.tokens, dictionary))
                    for s in raw]
        before, _ = build_word_stats(baseline)
        stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
        reserved = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact(raw))).data
        merged = [TokenizedSentence(s.text, s.tag, s.origin, combine_tokens_jmdict(s.tokens, dictionary))
                  for s in reserved]
        after, _ = build_word_stats(merged)
        self.assertEqual(stage.occurrences[RULE.id].count, 2)
        self.assertIn('とく', before)
        self.assertNotIn('とく', after)
        self.assertIn('おく', before)
        self.assertNotIn('おく', after)
        self.assertIn('ほる', after)
        self.assertIn('買う', after)
        self.assertIn('置く', after)
        components = list(after['ほる'].component_counts)
        self.assertEqual(components[0][0].surface, 'ほっ')
        self.assertEqual(components[0][0].lemma, 'ほる')

    def test_archived_overlapping_expressions_and_verb(self):
        from src.TokenizedSentence import TokenizedSentence
        from src.steps.TokenizeStep import tokenize_raw_sentences
        from src.steps.MergeTokensStep import combine_tokens_jmdict, get_jmdict
        from src.steps.BuildWordStatsStep import build_word_stats

        try:
            dictionary = get_jmdict()
            texts = ['そんなもの、放っておけばいい。',
                     'こんな事なら、ちゃんと用意しておけばよかったクマ。',
                     'おまえたちは班長である俺の命令にだけ従っておけばいいんだ',
                     '従って、結論は明白です。', '明日のために本を買っておくよ。']
            raw = tokenize_raw_sentences(texts, tag='source', source_path=Path('source.txt'))
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi or JMdict unavailable: {exc}')
        stage = GrammarStep(prefer_expressions=True)
        reserved = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact(raw))).data
        merged = [TokenizedSentence(s.text, s.tag, s.origin, combine_tokens_jmdict(s.tokens, dictionary))
                  for s in reserved]
        words, _ = build_word_stats(merged)
        self.assertEqual(words['おけばいい'].frequency, 2)
        self.assertEqual(words['おけばよかった'].frequency, 1)
        self.assertIn('放る', words)
        self.assertIn('従う', words)
        self.assertEqual(words['従う'].frequency, 1)
        self.assertEqual(words['従って'].frequency, 1)
        self.assertTrue(any(c.surface == '従っ' and c.lemma == '従う'
                            for parts in words['従う'].component_counts for c in parts))
        self.assertEqual([c.surface for c in next(iter(words['おけばよかった'].component_counts))],
                         ['おけ', 'ば', 'よかっ', 'た'])
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual([t['surface'] for t in reserved[-1].tokens if t.get('grammar_reserved')], ['て', 'おく'])


if __name__ == '__main__':
    unittest.main()
