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
                 '行ったことがない。': None, '食べたことがあった。': 'ことがあった',
                 '日本に行ったことがありました。': 'ことがありました',
                 'どこかで見たことがあったと思ったら。': 'ことがあった',
                 '行ったことがありません。': None,
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

