import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.tosuruto.index import RULE, match_tosuruto
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TosurutoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'雨が降るとすると出かけられない。': 'とすると',
                 '彼が犯人だとすると驚きだ。': 'とすると',
                 '高いとすると買えない。': 'とすると',
                 '彼を代表とすると決めた。': 'とすると',
                 '仕事をすると疲れる。': None,
                 '雨だと すると困る。': None,
                 'とすると困る。': None,
                 '雨だとすれば困る。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tosuruto(tokens)],
                                 [expected] if expected else [])

    def test_reservation_and_identity(self):
        source = sentence('雨だとすると明日の旅行は困る。', [
            tok('雨', '雨', ('名詞',)), tok('だ', 'だ', ('助動詞',)),
            tok('と', 'と', ('助詞',)), tok('する', 'する', ('動詞',)),
            tok('と', 'と', ('助詞',)), tok('明日', '明日', ('名詞',)),
            tok('の', 'の', ('助詞',)), tok('旅行', '旅行', ('名詞',)),
            tok('は', 'は', ('助詞',)), tok('困る', '困る', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertTrue(all(t.get('grammar_reserved') for t in artifact.data[0].tokens[2:5]))
        self.assertFalse(artifact.data[0].tokens[0].get('grammar_reserved', False))
        words = {'雨': object()}
        stage.add_cards(words)
        self.assertIn('雨', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とすると')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
