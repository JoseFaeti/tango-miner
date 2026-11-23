import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niseyo_nishiro.index import RULE as SINGLE_RULE
from src.grammar_rules.rules.niseyoniseyo.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NiseyoniseyoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨にせよ雪にせよ出かける。': ['にせよ'],
            '行くにしろ帰るにせよ決めて。': ['にしろ'],
            '雨にせよ、雪にしろ行く。': ['にせよ'],
            '猫にせよ犬にせよ鳥にせよ選ぶ。': ['にせよ', 'にせよ'],
            '雨にせよ出かける。': [],
            '雨にせよ。雪にせよ行く。': [],
            '雨に せよ雪にせよ行く。': [],
            '仕事をしろ。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(tokens)], expected)

    def test_overlap_reservation_and_identity(self):
        text = '雨にせよ雪にしろ出かける。'
        raw = [tok('雨', '雨', ('名詞',)), tok('に', 'に', ('助詞',)),
               tok('せよ', 'する', ('動詞',)), tok('雪', '雪', ('名詞',)),
               tok('に', 'に', ('助詞',)), tok('しろ', 'する', ('動詞',)),
               tok('出かける', '出かける', ('動詞',)), tok('。', '。', ('補助記号',))]
        stage = GrammarStep(rules=(RULE, SINGLE_RULE))
        artifact = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([sentence(text, raw)])))
        self.assertFalse(artifact.data[0].tokens[0].get('grammar_reserved'))
        self.assertFalse(artifact.data[0].tokens[3].get('grammar_reserved'))
        self.assertTrue(all(t.get('grammar_reserved') for t in artifact.data[0].tokens[1:3]))
        words = {'雨': object(), '雪': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にせよ')
        self.assertIn(SINGLE_RULE.expression, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
