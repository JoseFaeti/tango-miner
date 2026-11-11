import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.yori.index import RULE, match_yori
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class YoriTests(unittest.TestCase):
    def test_analyzer_comparisons_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'昨日より寒い。': ['より'], '猫より大きい。': ['より'],
                 '東京より来た。': [], 'より大きい。': [],
                 '料理を作る。': [], '猫より犬が好き。': [],
                 '猫 より寒い。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_yori(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('この犬は猫より大きいです。', [
            tok('この犬は', '犬', ('名詞',)), tok('猫', '猫', ('名詞',)), tok('より', 'より', ('助詞', '格助詞')),
            tok('大きい', '大きい', ('形容詞',)), tok('です。', 'です', ('助動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['より'])
        lexical_yori = object()
        words = {'猫': object(), '大きい': object(), 'より': lexical_yori}
        stage.add_cards(words)
        self.assertEqual(RULE.expression, '〜より')
        self.assertIs(words['より'], lexical_yori)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'より')
        self.assertIn('猫', words)
        self.assertIn('大きい', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
