import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.hazu_da import RULE, match_hazu_da
from tests.test_grammar_te_iru import sentence, tok


class HazuDaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降るはずだ。': 'はずだ', '来たはずです。': 'はずです',
            '寒いはずだった。': 'はずだった', '静かなはずでした。': 'はずでした',
            '行かないはずだ。': 'はずだ', '矢のはずだ。': None,
            '学生のはずだ。': None, '矢のはずを調べた。': None,
            '行くはずではない。': None, '行くはずの人。': None,
            '行くはずだっ': None, '行くはず だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_hazu_da(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card_example(self):
        source = sentence('明日は雨が降るはずだよ。', [
            tok('明日は雨が', '雨', ('名詞',)),
            tok('降る', '降る', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('はず', 'はず', ('名詞', '普通名詞', '一般')),
            tok('だ', 'だ', ('助動詞', '*', '*', '*', '助動詞-ダ', '終止形-一般')),
            tok('よ。', 'よ', ('助詞',))])
        self.assertIn(RULE, RULES)
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')], ['はず', 'だ'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {'降る': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'はずだ')
        self.assertIn('降る', words)


if __name__ == '__main__':
    unittest.main()
