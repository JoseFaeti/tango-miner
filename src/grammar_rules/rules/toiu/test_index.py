import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.toiu.index import RULE, match_toiu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToiuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'猫という動物': 'という', '猫っていう動物': 'っていう',
                 '東京という町': 'という', '行くという話': 'という',
                 '雨だっていう話': 'っていう', '猫って言う名前': 'って言う',
                 '彼は行くと言う。': None, 'そういう人': None,
                 '犬といったら猫': None, '猫と いう動物': None,
                 '猫というより犬': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiu(tokens)],
                                 [] if expected is None else [expected])

    def test_card_reserves_only_construction_and_keeps_components(self):
        source = sentence('猫という動物が好きです。', [tok('猫', '猫', ('名詞',)),
                          tok('と', 'と', ('助詞',)), tok('いう', 'いう', ('動詞',)),
                          tok('動物', '動物', ('名詞',)), tok('が好きです。', '好き', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'いう'])
        words = {'猫': object(), '動物': object()}
        stage.add_cards(words)
        self.assertIn('猫', words)
        self.assertIn('動物', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'という')


if __name__ == '__main__':
    unittest.main()
