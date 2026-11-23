import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tokoroda.index import match_tokoroda
from src.grammar_rules.rules.tokorodatsuta.index import match_tokorodatsuta


class TokorodatsutaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'もう少しで転ぶところだった。': ('ところだった', None),
            '危うく忘れる所でした。': ('所でした', None),
            '出かけるところだ。': (None, 'ところだ'),
            'さっき読んだところだった。': (None, 'ところだった'),
            '食べているところだった。': (None, 'ところだった'),
            '静かなところだった。': (None, None),
            '転ぶところだったら危ない。': (None, None),
        }
        for text, (nearly, general) in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                for matcher, expected in ((match_tokorodatsuta, nearly), (match_tokoroda, general)):
                    self.assertEqual([text[a:b] for a, b in matcher(aligned)],
                                     [] if expected is None else [expected], raw)

    def test_card_reserves_marker_and_preserves_verb(self):
        source = sentence('危うく転ぶところだった。', [
            tok('危うく', '危うい', ('形容詞',)),
            tok('転ぶ', '転ぶ', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('ところ', 'ところ', ('名詞', '普通名詞')),
            tok('だっ', 'だ', ('助動詞',)), tok('た', 'た', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ところ', 'だっ', 'た'])
        words = {'転ぶ': object()}
        stage.add_cards(words)
        self.assertIn('転ぶ', words)
        self.assertNotIn('〜ところだ', words)
        self.assertEqual(words['〜ところだった'].frequency, 2)
        self.assertEqual(words['〜ところだった'].sentences[0].surface_form, 'ところだった')
