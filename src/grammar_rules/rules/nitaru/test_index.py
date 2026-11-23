import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nitaru.index import RULE, match_nitaru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NitaruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'信頼に足る人物。': ['に足る'], '信頼にたる人物。': ['にたる'],
                 '議論するに足る問題。': ['に足る'], '水が生活に足る。': ['に足る'],
                 '水が足る。': [], '教師たる者。': [], '注目に足りない。': [],
                 '信頼に足った。': [], '信頼に 足る人物。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitaru(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('信頼に足る人物を探している。', [tok('信頼', '信頼', ('名詞',)),
                          tok('に', 'に', ('助詞',)), tok('足る', '足る', ('動詞',)),
                          tok('人物', '人物', ('名詞',)),
                          tok('を探している。', '探す', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '足る'])
        words = {'信頼': object()}
        stage.add_cards(words)
        self.assertIn('信頼', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に足る')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
