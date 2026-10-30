import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.koto_ga_aru_occasional import RULE, match_occasional
from tests.test_grammar_te_iru import sentence, tok


class OccasionalTests(unittest.TestCase):
    def test_analyzer_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'時々読むことがある。': 'ことがある',
                 'たまに読むことがあります。': 'ことがあります',
                 '読むことがある。': None, '時々聞きたいことがある。': None,
                 '時々読んだことがある。': None, '時々読むことがない。': None,
                 '時々読むことがあった。': 'ことがあった',
                 '時々読むことがありました。': 'ことがありました',
                 '時々読むことがありません。': None, '時々読む本がある。': None,
                 '時々雨が降ることがある。': 'ことがある',
                 '時には、人が変わってしまうことがあるのです。': 'ことがある',
                 '時々幸せだからこそ、悲しいと思うことがあるの。': 'ことがある',
                 '時々出かける。「聞くことがある。」': None,
                 '時々仕事をすることがある。': 'ことがある'}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_occasional(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_preserves_cue_verb_and_unmatched_lexical_use(self):
        verb = ('動詞', '一般', '*', '*', '五段', '連体形-一般')
        matched = sentence('時々読むことがある。', [tok('時々', '時々', ('名詞',)),
            tok('読む', '読む', verb), tok('ことがある', 'ことがある', ('名詞',)),
            tok('。', '。', ('補助記号',))])
        lexical = sentence('聞きたいことがある。', [tok('聞き', '聞く', ('動詞',)),
            tok('たい', 'たい', ('助動詞',)), tok('ことがある', 'ことがある', ('名詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([matched, lexical])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['ことがある'])
        self.assertFalse(any(t.get('grammar_reserved') for t in data.data[1].tokens))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertIn('読む', words)


if __name__ == '__main__':
    unittest.main()
