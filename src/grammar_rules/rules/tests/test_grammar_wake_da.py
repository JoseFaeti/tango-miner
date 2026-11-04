import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.wake_da import EXPLANATION, NEGATIVE
from src.grammar_rules.rules.tests.test_grammar_te_iru import sentence, tok


class WakeDaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降るわけだ。': (EXPLANATION, 'わけだ'),
            '静かなわけでした。': (EXPLANATION, 'わけでした'),
            '行くわけではないです。': (NEGATIVE, 'わけではないです'),
            '行くわけではなかったです。': (NEGATIVE, 'わけではなかったです'),
            '行く訳ではない。': (NEGATIVE, '訳ではない'),
            '行くわけじゃなかった。': (NEGATIVE, 'わけじゃなかった'),
            '行くわけではありません。': (NEGATIVE, 'わけではありません'),
            '行くわけじゃありませんでした。': (NEGATIVE, 'わけじゃありませんでした'),
            'そのわけだ。': None, '理由のわけだ。': None,
            '行くわけを聞いた。': None, '行くわけだっ': None,
            '行くわけ だ。': None, '行くわけではなかっ': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                found = [(rule, text[a:b]) for rule in (EXPLANATION, NEGATIVE)
                         for a, b in rule.match(aligned)]
                self.assertEqual(found, [] if expected is None else [expected])

    def test_reservation_and_card(self):
        source = sentence('今日は行くわけじゃないよ。', [
            tok('今日は', '今日', ('名詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('わけ', 'わけ', ('名詞', '普通名詞', '一般')),
            tok('じゃ', 'だ', ('助動詞', '*', '*', '*', '助動詞-ダ', '連用形-融合')),
            tok('ない', 'ない', ('形容詞', '非自立可能', '*', '*', '形容詞', '終止形-一般')),
            tok('よ。', 'よ', ('助詞',))])
        self.assertIn(NEGATIVE, RULES)
        stage = GrammarStep(rules=(NEGATIVE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['わけ', 'じゃ', 'ない'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[NEGATIVE.expression].sentences[0].surface_form, 'わけじゃない')
        self.assertIn('行く', words)


if __name__ == '__main__':
    unittest.main()
