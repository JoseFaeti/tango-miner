import unittest
from unittest.mock import patch

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep, GrammarRule
from src.grammar_rules.rules.te_kuru import RULE
from src.grammar_rules.rules.nda import RULE as NDA_RULE
from src.steps.MergeTokensStep import combine_tokens_jmdict
from src.steps.BuildWordStatsStep import build_word_stats
from tests.test_grammar_te_iru import sentence, tok


class Dictionary:
    def lookup_word(self, word):
        # A tempting dictionary match must not cross the grammar span.
        if word == '降ってきた':
            raise AssertionError('merge crossed a grammar span')
        return []


class GrammarReservationTests(unittest.TestCase):
    def test_grammar_reserved_before_merge_but_other_uses_survive(self):
        grammar_source = sentence('外では雨が降ってきたよ。', [
            tok('外では雨が', '雨', ('名詞', '普通名詞')),
            tok('降っ', '降る', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('き', 'くる', ('動詞', '非自立可能')),
            tok('た', 'た', ('助動詞', '*')),
            tok('よ。', 'よ', ('助詞', '*'))])
        lexical_source = sentence('友達が家に来たよ。', [
            tok('友達が家に', '友達', ('名詞', '普通名詞')),
            tok('来', 'くる', ('動詞', '非自立可能')),
            tok('たよ。', 'た', ('助動詞', '*'))])
        for source in (grammar_source, lexical_source):
            for token in source.tokens:
                token.update(base_form=token["lemma"], reading="ヨミ")
        stage = GrammarStep(rules=(RULE,))
        data = Artifact([grammar_source, lexical_source])
        stage.process(data)
        data = ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['て', 'き', 'た'])
        self.assertFalse(any(t.get('grammar_reserved') for t in data.data[1].tokens))
        merged = [sentence(s.text, combine_tokens_jmdict(s.tokens, Dictionary())) for s in data.data]
        words, _ = build_word_stats(merged)
        self.assertEqual(words['くる'].frequency, 1)
        self.assertEqual(words['降る'].frequency, 1)
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)

    def test_complete_dictionary_expression_takes_precedence(self):
        source = sentence('私はいってきますね。', [
            tok('私は', '私', ('名詞', '普通名詞')),
            tok('いっ', 'いく', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('き', 'くる', ('動詞', '非自立可能')),
            tok('ます', 'ます', ('助動詞', '*')),
            tok('ね。', 'ね', ('助詞', '*'))])
        merged = [source.tokens[0], {'surface': 'いってきます', 'lemma': 'いってきます',
                  'pos': ('exp', '*'), 'analysis_rule': 'jmdict_surface_merge'}, source.tokens[-1]]
        stage = GrammarStep(rules=(RULE,), prefer_expressions=True)
        with patch('src.steps.MergeTokensStep.get_jmdict'), \
             patch('src.steps.MergeTokensStep.combine_tokens_jmdict', return_value=merged):
            stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[RULE.id].count, 0)
        self.assertEqual(stage.spans, {})

    def test_negative_at_expression_start_preserves_expression_and_other_negatives(self):
        from src.grammar_rules.rules.nai import RULE as NAI_RULE
        source = sentence('本を読まないといけないが遊ばない。', [
            tok('本を', '本', ('名詞', '普通名詞')),
            tok('読ま', '読む', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞', '*')),
            tok('と', 'と', ('助詞', '*')),
            tok('いけ', 'いける', ('動詞', '一般', '*', '*', '下一段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞', '*')),
            tok('が', 'が', ('助詞', '*')),
            tok('遊ば', '遊ぶ', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞', '*')),
            tok('。', '。', ('補助記号', '*'))])
        for token in source.tokens:
            token.update(base_form=token['lemma'], reading='ヨミ')
        entry = {'senses': [{'pos': ['expressions (phrases, clauses, etc.)']}]}
        class ExpressionDictionary(Dictionary):
            def lookup_word(self, word): return [entry] if word == 'ないといけない' else []
            def get_best_entry(self, word): return entry
            def get_reading(self, entry, word): return word
        dictionary = ExpressionDictionary()
        stage = GrammarStep(rules=(NAI_RULE,), prefer_expressions=True)
        with patch('src.steps.MergeTokensStep.get_jmdict', return_value=dictionary):
            artifact = stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[NAI_RULE.id].count, 1)
        tokens = ExcludeGrammarTokensStep(stage).process(artifact).data[0].tokens
        self.assertEqual([t['surface'] for t in tokens if t.get('grammar_reserved')], ['ない'])
        merged = combine_tokens_jmdict(tokens, dictionary)
        expression = next(t for t in merged if t['lemma'] == 'ないといけない')
        self.assertEqual([c.surface for c in expression['components']], ['ない', 'と', 'いけ', 'ない'])
        self.assertTrue(any(t['lemma'] == '読む' for t in merged))
        self.assertTrue(any(t['lemma'] == '遊ぶ' for t in merged))

    def test_explanatory_copula_overrides_misleading_dictionary_merge(self):
        source = sentence('葉っぱにはかけちゃだめなんだって。', [
            tok('葉っぱにはかけちゃだめ', 'だめ', ('名詞', '普通名詞')),
            tok('な', 'だ', ('助動詞', '*')),
            tok('ん', 'ん', ('助詞', '準体助詞')),
            tok('だ', 'だ', ('助動詞', '*')),
            tok('って', 'って', ('助詞', '副助詞')),
            tok('。', '。', ('補助記号', '*'))])
        merged = [source.tokens[0], {'surface': 'なんだって', 'lemma': 'なんだって',
                  'pos': ('exp', '*'), 'analysis_rule': 'jmdict_surface_merge'}, source.tokens[-1]]
        stage = GrammarStep(rules=(NDA_RULE,), prefer_expressions=True)
        with patch('src.steps.MergeTokensStep.get_jmdict'), \
             patch('src.steps.MergeTokensStep.combine_tokens_jmdict', return_value=merged):
            artifact = stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[NDA_RULE.id].count, 1)
        self.assertEqual(stage.occurrences[NDA_RULE.id].examples[0].surface_form, 'んだ')
        reserved = ExcludeGrammarTokensStep(stage).process(artifact).data[0].tokens
        self.assertEqual([t['surface'] for t in reserved if t.get('grammar_reserved')], ['ん', 'だ'])
        self.assertFalse(reserved[1].get('grammar_reserved'))

    def test_longer_sou_nandesuka_expression_keeps_precedence(self):
        source = sentence('ああそうなんですか。', [
            tok('ああ', 'ああ', ('感動詞', '*')),
            tok('そう', 'そう', ('副詞', '*')), tok('な', 'だ', ('助動詞', '*')),
            tok('ん', 'ん', ('助詞', '準体助詞')), tok('です', 'です', ('助動詞', '*')),
            tok('か', 'か', ('助詞', '*')), tok('。', '。', ('補助記号', '*'))])
        merged = [source.tokens[0], {'surface': 'そうなんですか', 'lemma': 'そうなんですか',
                  'pos': ('exp', '*'), 'analysis_rule': 'jmdict_surface_merge'}, source.tokens[-1]]
        stage = GrammarStep(rules=(NDA_RULE,), prefer_expressions=True)
        with patch('src.steps.MergeTokensStep.get_jmdict'), \
             patch('src.steps.MergeTokensStep.combine_tokens_jmdict', return_value=merged):
            stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[NDA_RULE.id].count, 0)

    def test_interrogative_nandatte_remains_available(self):
        source = sentence('なんだってできるんだよ。', [
            tok('なん', 'なん', ('代名詞', '*')), tok('だ', 'だ', ('助動詞', '*')),
            tok('って', 'って', ('助詞', '副助詞')),
            tok('できる', 'できる', ('動詞', '一般')),
            tok('ん', 'ん', ('助詞', '準体助詞')), tok('だ', 'だ', ('助動詞', '*')),
            tok('よ。', 'よ', ('助詞', '*'))])
        stage = GrammarStep(rules=(NDA_RULE,))
        artifact = stage.process(Artifact([source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact).data[0].tokens
        self.assertFalse(any(t.get('grammar_reserved') for t in reserved[:3]))
        self.assertEqual([t['surface'] for t in reserved if t.get('grammar_reserved')], ['ん', 'だ'])

    def test_inflected_me_ni_mieru_recovers_expression_without_grammar_overlap(self):
        source = sentence('この先の結果は目に見えているよ。', [
            tok('この先の結果は', '結果', ('名詞', '普通名詞')),
            tok('目', '目', ('名詞', '普通名詞')), tok('に', 'に', ('助詞', '格助詞')),
            tok('見え', '見える', ('動詞', '一般', '*', '*', '下一段', '連用形-一般')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('いる', 'いる', ('動詞', '非自立可能')), tok('よ。', 'よ', ('助詞', '*'))])
        for token in source.tokens:
            token.update(base_form=token['lemma'], reading='ヨミ')
        from src.grammar_rules.rules.te_iru import RULE as IRU_RULE
        stage = GrammarStep(rules=(IRU_RULE,))
        art = stage.process(Artifact([source]))
        tokens = ExcludeGrammarTokensStep(stage).process(art).data[0].tokens
        entry = {'senses': [{'pos': ['expressions (phrases, clauses, etc.)']}]}
        class ExpressionDictionary(Dictionary):
            def lookup_word(self, word): return [entry] if word == '目に見える' else []
            def get_best_entry(self, word): return entry
            def get_reading(self, entry, word): return 'めにみえる'
        merged = combine_tokens_jmdict(tokens, ExpressionDictionary())
        expression = next(t for t in merged if t['lemma'] == '目に見える')
        self.assertEqual(expression['surface'], '目に見え')
        self.assertTrue(expression['dictionary_expression'])
        self.assertEqual([t['surface'] for t in merged if t.get('grammar_reserved')], ['て', 'いる'])
        self.assertEqual(stage.occurrences[IRU_RULE.id].count, 1)
        self.assertEqual(len(expression['components']), 3)
        ordinary = [{k: v for k, v in token.items() if k != 'grammar_reserved'} for token in tokens]
        self.assertFalse(any(t['lemma'] == '目に見える'
                             for t in combine_tokens_jmdict(ordinary, ExpressionDictionary())))

    def test_kigahen_repair_does_not_change_reserved_components(self):
        from src.steps.MergeTokensStep import _repair_kigahen_boundaries
        tokens = [dict(surface='気が変', lemma='気が変', pos=('形状詞',), grammar_reserved=True),
                  dict(surface='わっ', lemma='わる', pos=('動詞',)),
                  dict(surface='たら', lemma='た', pos=('助動詞',), grammar_reserved=True)]
        class PresentDictionary:
            def lookup_word(self, word): return [word]
        self.assertEqual(_repair_kigahen_boundaries(tokens, PresentDictionary()), tokens)

    def test_passive_guard_sees_reserved_connective(self):
        from src.steps.MergeTokensStep import _is_inflection_misread_as_noun
        tokens = [dict(surface='覆わ', lemma='覆う', pos=('動詞', '*', '*', '*', '*', '未然形')),
                  dict(surface='れ', lemma='れる', pos=('助動詞', '*')),
                  dict(surface='て', lemma='て', pos=('助詞', '接続助詞'), grammar_reserved=True)]
        entry = {'senses': [{'pos': ['noun (common)']}]}
        self.assertTrue(_is_inflection_misread_as_noun(tokens, 0, 2, entry))

        class PassiveDictionary(Dictionary):
            def lookup_word(self, word):
                return [entry] if word == '覆われ' else []
            def get_best_entry(self, word):
                return entry
        for token in tokens:
            token.update(base_form=token['lemma'], reading='ヨミ')
        result = combine_tokens_jmdict(tokens, PassiveDictionary())
        self.assertEqual([t['surface'] for t in result], ['覆わ', 'れ', 'て'])

    def test_unaligned_or_inactive_rule_does_not_consume_tokens(self):
        source = sentence('短い。', [tok('短い', '短い', ('形容詞', '一般')), tok('。', '。', ('補助記号', '*'))])
        rule = GrammarRule('test', 'test', 'test', 'test', lambda tokens: [(0, 2)])
        stage = GrammarStep(rules=(rule,))
        artifact = Artifact([source])
        stage.process(artifact)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(any(t.get('grammar_reserved') for t in artifact.data[0].tokens))


if __name__ == '__main__':
    unittest.main()
