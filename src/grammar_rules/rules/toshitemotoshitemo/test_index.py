import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.toshitemotoshitemo.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class PairedToshitemoTests(unittest.TestCase):
    def test_analyzer_forms_and_boundaries(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'雨としても雪としても出かける。': 1,
                 '行くとしても、行かないとしても連絡して。': 1,
                 '雨としても雪としても風としても困る。': 2,
                 '雨としても出かける。': 0,
                 '雨としても。雪としても困る。': 0,
                 '仕事をしても終わらない。': 0}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(tokens)],
                                 ['としても'] * expected)

    def test_reservation_and_identity(self):
        text = '雨としても雪としても出かける。'
        raw = [tok('雨', '雨', ('名詞',)), tok('と', 'と', ('助詞',)),
               tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
               tok('も', 'も', ('助詞',)), tok('雪', '雪', ('名詞',)),
               tok('と', 'と', ('助詞',)), tok('し', 'する', ('動詞',)),
               tok('て', 'て', ('助詞',)), tok('も', 'も', ('助詞',)),
               tok('出かける', '出かける', ('動詞',)), tok('。', '。', ('補助記号',))]
        stage = GrammarStep()
        artifact = stage.process(Artifact([sentence(text, raw)]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[5].get('grammar_reserved'))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual(stage.occurrences['toshitemo'].count, 2)
        words = {'雪': object()}
        stage.add_cards(words)
        self.assertIn('雪', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'としても')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
