import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.you_ni import RULE, match_you_ni
from tests.test_grammar_te_iru import sentence, tok


class YouNiTests(unittest.TestCase):
    def test_observed_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'忘れないようにメモする。': True, '読めるように練習する。': True,
                 '先生が言ったように書く。': True, '早く来るように言った。': True,
                 '雨が降るように見える。': True, '彼のように走る。': False,
                 'このようにする。': False, '読むように。': False}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_you_ni(aligned)],
                                 ['ように'] if expected else [])

    def test_reserves_only_construction(self):
        source = sentence('今日は本を読むように言うよ。', [
            tok('今日は本を', '本', ('名詞',)),
            tok('読む', '読む', ('動詞', '*', '*', '*', '五段', '連体形-一般')),
            tok('よう', 'よう', ('形状詞', '助動詞語幹')),
            tok('に', 'だ', ('助動詞', '*', '*', '*', '助動詞-ダ', '連用形-ニ')),
            tok('言う', '言う', ('動詞',)), tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['よう', 'に'])
        words = {'読む': object(), '言う': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ように')
        self.assertIn('読む', words)
        self.assertIn('言う', words)


if __name__ == '__main__':
    unittest.main()
