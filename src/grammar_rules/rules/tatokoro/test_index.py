import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import TATOKORO_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tatokoro.index import RULE, match_tatokoro
from src.grammar_rules.rules.tokoroda.index import RULE as TOKORODA
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TatokoroTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '調べたところ、間違いが見つかった。': 'ところ',
            '読んだところ、面白かった。': 'ところ',
            '今帰ったところだ。': 'ところ',
            '昨日行った所を見た。': '所',
            '食べるところだ。': None,
            '静かなところだ。': None,
            'ところで話がある。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tatokoro(aligned)],
                                 [] if expected is None else [expected], raw)

    def test_card_reservation_and_copular_overlap(self):
        source = sentence('私は今家に帰ったところだ。', [
            tok('私は今家に', '今', ('名詞',)), tok('帰っ', '帰る', ('動詞',)),
            tok('た', 'た', ('助動詞',)), tok('ところ', 'ところ', ('名詞',)),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, TOKORODA))
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ところ', 'だ'])
        words = {'帰る': object()}
        stage.add_cards(words)
        self.assertEqual(words[TATOKORO_EXPRESSION].frequency, 2)
        self.assertEqual(words[TATOKORO_EXPRESSION].sentences[0].surface_form, 'ところ')
        self.assertIn(TOKORODA.expression, words)
        self.assertIn('帰る', words)


if __name__ == '__main__':
    unittest.main()
