import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ohajime.index import RULE, match_ohajime
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OhajimeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '東京をはじめ各地で開催する。': 'をはじめ',
            '日本を始め、多くの国が参加した。': 'を始め',
            '東京をはじめとする都市。': 'をはじめ',
            '東京をはじめとして各地で開催する。': 'をはじめ',
            '仕事を始めた。': None,
            '仕事を始めて帰る。': None,
            '本のはじめを読む。': None,
            '東京を はじめ各地で開催する。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ohajime(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('東京をはじめ各地で開催する。', [
            tok('東京', '東京', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('はじめ', 'はじめ', ('名詞',)), tok('各地', '各地', ('名詞',)),
            tok('で開催する。', 'で', ('助詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'はじめ'])
        words = {'東京': object(), '各地': object()}
        stage.add_cards(words)
        self.assertIn('東京', words)
        self.assertIn('各地', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をはじめ')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
