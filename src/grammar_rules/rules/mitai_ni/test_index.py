import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.mitai_ni.index import RULE, match_mitai_ni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MitaiNiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '猫みたいに走る。': 'みたいに', '猫みたいな人。': 'みたいな',
            '彼みたいに話す。': 'みたいに', '雨が降るみたいに見える。': 'みたいに',
            '夢みたいに美しい。': 'みたいに', '見たいニュース。': None,
            '猫みたいだ。': None, 'みたいな人。': None, '猫 みたいな人。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_mitai_ni(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card(self):
        source = sentence('あの人は猫みたいな人です。', [
            tok('あの人は', '人', ('名詞',)),
            tok('猫', '猫', ('名詞',)),
            tok('みたい', 'みたい', ('形状詞', '助動詞語幹')),
            tok('な', 'だ', ('助動詞',)), tok('人です。', '人', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['みたい', 'な'])
        words = {'猫': object()}
        stage.add_cards(words)
        self.assertIn('猫', words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'みたいな')

    def test_wrong_ending_analysis(self):
        parts = [tok('猫', '猫', ('名詞',)), tok('みたい', 'みたい', ('形状詞', '助動詞語幹')),
                 tok('に', 'に', ('助詞',))]
        aligned, error = aligned_tokens('猫みたいに', parts)
        self.assertIsNone(error)
        self.assertEqual(list(match_mitai_ni(aligned)), [])
