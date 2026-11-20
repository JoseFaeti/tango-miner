import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niseyo_nishiro.index import RULE, match_niseyo_nishiro
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiseyoNishiroTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨にせよ出かける。': ['にせよ'],
            '行くにしろ時間がない。': ['にしろ'],
            '高いにせよ買いたい。': ['にせよ'],
            '何にしろ早い。': ['にしろ'],
            '雨にせよ雪にしろ行く。': ['にせよ', 'にしろ'],
            '彼にしろ。': ['にしろ'],  # Also a literal imperative.
            '雨に せよ出かける。': [],
            'にせよ、無理だ。': [],
            '仕事をしろ。': [],
            '子供にしても無理だ。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niseyo_nishiro(tokens)], expected)

    def test_boundaries_and_lexical_controls(self):
        for text, raw in [
            ('雨にしろい', [tok('雨', '雨', ('名詞',)), tok('に', 'に', ('助詞',)),
                        tok('しろい', '白い', ('形容詞',))]),
            ('雨、にせよ', [tok('雨', '雨', ('名詞',)), tok('、', '、', ('補助記号',)),
                        tok('に', 'に', ('助詞',)), tok('せよ', 'する', ('動詞',))]),
        ]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_niseyo_nishiro(tokens)), [])

    def test_reservation_and_identity(self):
        source = sentence('雨にせよ明日は出かける予定だ。', [
            tok('雨', '雨', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('せよ', 'する', ('動詞',)), tok('明日', '明日', ('名詞',)),
            tok('は', 'は', ('助詞',)), tok('出かける', '出かける', ('動詞',)),
            tok('予定', '予定', ('名詞',)), tok('だ', 'だ', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertTrue(all(t.get('grammar_reserved') for t in artifact.data[0].tokens[1:3]))
        self.assertFalse(artifact.data[0].tokens[0].get('grammar_reserved', False))
        words = {'雨': object()}
        stage.add_cards(words)
        self.assertIn('雨', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にせよ')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
