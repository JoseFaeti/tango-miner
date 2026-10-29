import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.koto_ga_aru import RULE, match_experience
from tests.test_grammar_te_iru import sentence, tok


class ExperienceTests(unittest.TestCase):
    def test_configured_analyzer_examples_and_ambiguities(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'日本へ行ったことがある。': 'ことがある',
                 '本を読んだことがあります。': 'ことがあります',
                 '雨が降ることがある。': None, 'そんなことがあった。': None,
                 '行ったことがない。': 'ことがない',
                 '日本へ行ったことがなかった。': 'ことがなかった',
                 '日本へ行ったことがありません。': 'ことがありません',
                 '日本へ行ったことがありませんでした。': 'ことがありませんでした',
                 'わしはブーメランを見たことがない。': 'ことがない',
                 'やることがないなら宿屋へ。': None,
                 '聞きたいことがない。': None,
                 '食べたことがあった。': 'ことがあった',
                 '日本に行ったことがありました。': 'ことがありました',
                 'どこかで見たことがあったと思ったら。': 'ことがあった',
                 '行ったことがありません。': 'ことがありません',
                 '困ったことがあったら相談して。': None,
                 '言い忘れたことがあったわ。': None,
                 'アークがいなくなってわかったことがあった。': None,
                 '彼の気持ちがわかったことがあった。': 'ことがあった',
                 '困ったことがあるなら相談して。': None,
                 '彼の態度に困ったことがあるなら教えて。': 'ことがある',
                 '困ったことがある。': 'ことがある',
                 '言い忘れたことがあるわ。': None,
                 '言い忘れたことがある。': 'ことがある',
                 '約束を忘れたことがあるわ。': 'ことがある'}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_experience(aligned)],
                                 [] if expected is None else [expected])

    def test_negative_reservation_keeps_verb_and_lexical_control(self):
        verb = ('動詞', '一般', '*', '*', '五段', '連用形-促音便')
        matched = sentence('まだ本を読んだことがない。', [
            tok('まだ本を', '本', ('名詞',)), tok('読ん', '読む', verb),
            tok('だ', 'だ', ('助動詞',)), tok('ことがない', 'ことがない', ('形容詞',)),
            tok('。', '。', ('補助記号',))])
        lexical = sentence('聞きたいことがない。', [
            tok('聞き', '聞く', verb), tok('たい', 'たい', ('助動詞',)),
            tok('ことがない', 'ことがない', ('形容詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([matched, lexical])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['ことがない'])
        self.assertFalse(any(t.get('grammar_reserved') for t in data.data[1].tokens))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertIn('negative', words[RULE.expression].definition)

    def test_shared_negative_helper_accepts_opted_in_verb_stems(self):
        from src.grammar_rules.helpers import negative_predicate_end
        tokens = [dict(surface='いか', lemma='いく', pos=['動詞'], span=[0, 2]),
                  dict(surface='なかっ', lemma='ない', pos=['助動詞'], span=[2, 5]),
                  dict(surface='た', lemma='た', pos=['助動詞'], span=[5, 6])]
        args = dict(polite_lemma='いく', polite_stems={'いき'},
                    plain_lemma='いく', plain_stems={'いか'})
        self.assertEqual(negative_predicate_end(tokens, 0, **args), 2)
        self.assertIsNone(negative_predicate_end(tokens[:-1], 0, **args))
        self.assertIsNone(negative_predicate_end(tokens, 0, polite_lemma='いく', polite_stems={'いき'}))

    def test_incomplete_negative_suffix_is_not_reserved(self):
        from src.grammar_rules.helpers import negative_predicate_end
        tokens = [dict(surface='あり', lemma='ある', pos=['動詞'], span=[0, 2]),
                  dict(surface='ませ', lemma='ます', pos=['助動詞'], span=[2, 4]),
                  dict(surface='ん', lemma='ぬ', pos=['助動詞'], span=[4, 5]),
                  dict(surface='でし', lemma='です', pos=['助動詞'], span=[5, 7])]
        self.assertIsNone(negative_predicate_end(tokens, 0, polite_lemma='ある', polite_stems={'あり'}))
        tokens.pop()
        self.assertEqual(negative_predicate_end(tokens, 0, polite_lemma='ある', polite_stems={'あり'}), 2)
        tokens[2]['span'] = [5, 6]
        self.assertIsNone(negative_predicate_end(tokens, 0, polite_lemma='ある', polite_stems={'あり'}))

    def test_problem_expression_remains_unreserved_when_other_match_is_active(self):
        problem = sentence('困ったことがあるなら相談して。', [
            tok('困っ', '困る', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('た', 'た', ('助動詞', '*')),
            tok('ことがある', 'ことがある', ('名詞',)),
            tok('なら', 'だ', ('助動詞', '*')), tok('相談して。', '相談', ('名詞',))])
        experience = sentence('本を読んだことがあるよ。', [
            tok('本を', '本', ('名詞',)),
            tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            tok('だ', 'だ', ('助動詞', '*')), tok('ことがある', 'ことがある', ('名詞',)),
            tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([problem, experience])))
        self.assertFalse(any(t.get('grammar_reserved') for t in data.data[0].tokens))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertTrue(data.data[1].tokens[3].get('grammar_reserved'))

    def test_problem_boundary_recovers_dictionary_expression(self):
        from src.steps.MergeTokensStep import combine_tokens_jmdict
        class Dictionary:
            def lookup_word(self, word):
                return [{'senses': [{'pos': ['expressions (phrases, clauses, etc.)']}]}] if word == '困ったこと' else []
            def get_best_entry(self, word): return self.lookup_word(word)[0]
            def get_reading(self, entry, word): return 'こまったこと'
        tokens = [tok('困っ', '困る', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
                  tok('た', 'た', ('助動詞', '*')), tok('ことがある', 'ことがある', ('名詞',)),
                  tok('なら', 'だ', ('助動詞', '*'))]
        for token in tokens:
            token.update(base_form=token['lemma'], reading=token['lemma'])
        merged = combine_tokens_jmdict(tokens, Dictionary())
        self.assertEqual([t['surface'] for t in merged], ['困ったこと', 'が', 'ある', 'なら'])
        self.assertEqual(merged[0]['lemma'], '困ったこと')
        self.assertEqual([c.surface for c in merged[0]['components']], ['困っ', 'た', 'ことがある'])

    def test_reserves_only_construction_and_keeps_verb(self):
        source = sentence('本を読んだことがある。', [
            tok('本を', '本', ('名詞',)),
            tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            tok('だ', 'だ', ('助動詞', '*', '*', '*', '助動詞-タ', '連体形-一般')),
            tok('ことがある', 'ことがある', ('名詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['ことがある'])
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ことがある')


if __name__ == '__main__':
    unittest.main()

