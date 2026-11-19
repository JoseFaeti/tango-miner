import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.kagirida.index import RULE, match_kagirida
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KagiridaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '嬉しい限りだ。': '限りだ',
            '寂しい限りです。': '限りです',
            '残念な限りだ。': '限りだ',
            '嬉しいかぎりだ。': 'かぎりだ',
            '命には限りがある。': None,
            'これが限りだ。': None,
            'できる限りだ。': None,
            '嬉しい限りではない。': None,
            '嬉しい限りだった。': None,
            '嬉しい 限りだ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kagirida(tokens)],
                                 [] if expected is None else [expected], raw)

    def test_reservation_and_identity(self):
        source = sentence('皆さんのおかげで嬉しい限りだ。', [
            tok('皆さんのおかげで', '皆さん', ('名詞',)),
            tok('嬉しい', '嬉しい', ('形容詞', '一般', '*', '*', '形容詞', '連体形-一般')),
            tok('限り', '限り', ('名詞', '普通名詞')),
            tok('だ', 'だ', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['限り', 'だ'])
        words = {'嬉しい': object()}
        stage.add_cards(words)
        self.assertIn('嬉しい', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '限りだ')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
