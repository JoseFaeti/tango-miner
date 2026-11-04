import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import TOKORODA_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tokoroda.index import match_tokoroda
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TokorodaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '今から食べるところだ。': 'ところだ',
            '今食べているところです。': 'ところです',
            '今食べたところだ。': 'ところだ',
            'さっき読んだところだった。': 'ところだった',
            '出かける所でした。': '所でした',
            'そこは静かなところだ。': None,
            'ここは学校のところだ。': None,
            'ところで話がある。': None,
            '食べるところに行く。': None,
            '食べるところだったら行く。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tokoroda(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_card_preserves_verb_and_evidence(self):
        source = sentence('私は今から食べるところだ。', [
            tok('私は今から', '今', ('名詞',)),
            tok('食べる', '食べる', ('動詞', '一般', '*', '*', '下一段', '連体形-一般')),
            tok('ところ', 'ところ', ('名詞', '普通名詞')),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ところ', 'だ'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertEqual(words[TOKORODA_EXPRESSION].frequency, 2)
        self.assertEqual(words[TOKORODA_EXPRESSION].sentences[0].surface_form, 'ところだ')


if __name__ == '__main__':
    unittest.main()
