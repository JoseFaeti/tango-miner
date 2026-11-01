import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.sou_da import APPEARANCE, HEARSAY
from tests.test_grammar_te_iru import sentence, tok


class SouDaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降りそうだ。': ('そうだ', None),
            'おいしそうです。': ('そうです', None),
            '元気そうだった。': ('そうだった', None),
            '良さそうだ。': ('そうだ', None),
            '雨が降るそうだ。': (None, 'そうだ'),
            '雨が降ったそうです。': (None, 'そうです'),
            '彼は学生だそうです。': (None, 'そうです'),
            'そうだね。': (None, None), 'そうだと思う。': (None, None),
            '雨が降りそうにない。': (None, None),
            '雨が降りそうな空だ。': (None, None),
            '雨が降りそう だ。': (None, None),
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                for rule, want in zip((APPEARANCE, HEARSAY), expected):
                    self.assertEqual([text[a:b] for a, b in rule.match(aligned)],
                                     [] if want is None else [want])

    def test_reservation_and_examples(self):
        source = sentence('明日は雨が降りそうだよ。', [
            tok('明日は雨が', '雨', ('名詞',)),
            tok('降り', '降る', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('そう', 'そう', ('形状詞', '助動詞語幹')),
            tok('だ', 'だ', ('助動詞', '*', '*', '*', '助動詞-ダ', '終止形-一般')),
            tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep(rules=(APPEARANCE, HEARSAY))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')], ['そう', 'だ'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {'降る': object()}
        stage.add_cards(words)
        self.assertEqual(words[APPEARANCE.expression].sentences[0].surface_form, 'そうだ')
        self.assertIn('降る', words)
        self.assertNotEqual(APPEARANCE.expression, HEARSAY.expression)


if __name__ == '__main__':
    unittest.main()
