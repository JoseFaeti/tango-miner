import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.narinari.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NarinariTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '電話なりメールなりで知らせてください。': ['なり'],
            '行くなり帰るなり好きにして。': ['なり'],
            '猫なり、犬なり、鳥なりを選ぶ。': ['なり', 'なり'],
            '猫なりを選ぶ。': [], '猫なり。犬なりを選ぶ。': [],
            '猫 なり犬なりを選ぶ。': [], '大きくなり小さくなりする。': [],
            '彼なりの方法なり理由なりがある。': ['なり'],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(tokens)], expected,
                                 [(t['surface'], t['pos']) for t in tokens])

    def test_reservation_and_identity(self):
        text = '猫なり犬なりを選ぶ。'
        raw = [tok('猫', '猫', ('名詞',)), tok('なり', 'なり', ('助詞',)),
               tok('犬', '犬', ('名詞',)), tok('なり', 'なり', ('接尾辞',)),
               tok('を', 'を', ('助詞',)), tok('選ぶ', '選ぶ', ('動詞',)),
               tok('。', '。', ('補助記号',))]
        stage = GrammarStep(rules=(RULE,))
        artifact = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([sentence(text, raw)])))
        self.assertFalse(artifact.data[0].tokens[2].get('grammar_reserved'))
        words = {'犬': object()}
        stage.add_cards(words)
        self.assertIn('犬', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'なり')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
