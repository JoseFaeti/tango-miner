import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.tsuke.index import RULE, match_tsuke
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TsukeTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行ったっけ。': ['っけ'], '何だっけ？': ['っけ'],
                 'いつ来るっけ。': ['っけ'], '大きかったっけ。': ['っけ'],
                 'っけ？': [], '行く っけ。': [], 'きっかけ。': [], '付ける。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tsuke(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('これはかわいい猫だっけ。', [tok('これ', 'これ', ('代名詞',)),
                          tok('は', 'は', ('助詞', '係助詞')), tok('かわいい', 'かわいい', ('形容詞',)),
                          tok('猫', '猫', ('名詞',)),
                          tok('だ', 'だ', ('助動詞',)), tok('っけ', 'っけ', ('助詞', '終助詞')),
                          tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['っけ'])
        words = {'猫': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'っけ')
        self.assertIn('猫', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_other_part_of_speech(self):
        tokens, error = aligned_tokens('猫っけ', [tok('猫', '猫', ('名詞',)),
                                       tok('っけ', 'っけ', ('名詞',))])
        self.assertIsNone(error)
        self.assertEqual(list(match_tsuke(tokens)), [])
