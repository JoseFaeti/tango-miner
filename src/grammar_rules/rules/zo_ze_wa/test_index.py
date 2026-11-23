import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.zo_ze_wa.index import RULE, match_zo_ze_wa
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class ZoZeWaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行くぞ。': ['ぞ'], '行こうぜ。': ['ぜ'], 'そうだわ。': ['わ'],
                 '行くぞ、急げ。': ['ぞ'], 'これは本だ。': [], '庭を見る。': [],
                 '行く ぞ。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zo_ze_wa(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('これはかわいい猫だわ。', [tok('これ', 'これ', ('代名詞',)),
                          tok('は', 'は', ('助詞', '係助詞')), tok('かわいい', 'かわいい', ('形容詞',)),
                          tok('猫', '猫', ('名詞',)), tok('だ', 'だ', ('助動詞',)),
                          tok('わ', 'わ', ('助詞', '終助詞')), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['わ'])
        words = {'猫': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'わ')
        self.assertIn('猫', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_other_part_of_speech_and_initial_particle(self):
        for text, raw in [('猫わ', [tok('猫', '猫', ('名詞',)), tok('わ', 'わ', ('名詞',))]),
                          ('ぞ', [tok('ぞ', 'ぞ', ('助詞', '終助詞'))])]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_zo_ze_wa(tokens)), [])
