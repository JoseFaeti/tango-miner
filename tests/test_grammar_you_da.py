import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.you_da import RULE, match_you_da
from tests.test_grammar_te_iru import sentence, tok


class YouDaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降るようだ。': 'ようだ', '雨が降ったようです。': 'ようです',
            '彼は元気なようだ。': 'ようだ', '彼は学生のようだ。': 'ようだ',
            '雨が降るようだった。': 'ようだった', '雨が降るようでした。': 'ようでした',
            'バラモスがいなくなったなんてまるで夢のようですわ！': 'ようです',
            'このようだ。': None, '用だ。': None, '雨が降るような気がする。': None,
            '雨が降るようではない。': None, '雨が降るようで困る。': None,
            '雨が降るように見える。': None, '雨が降るよう だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_you_da(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card_example(self):
        source = sentence('明日は雨が降るようだよ。', [
            tok('明日は雨が', '雨', ('名詞',)),
            tok('降る', '降る', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('よう', 'よう', ('形状詞', '助動詞語幹')),
            tok('だ', 'だ', ('助動詞', '*', '*', '*', '助動詞-ダ', '終止形-一般')),
            tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')], ['よう', 'だ'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {'降る': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ようだ')
        self.assertIn('降る', words)


if __name__ == '__main__':
    unittest.main()
