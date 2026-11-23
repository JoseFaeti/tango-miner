import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ohajimetoshite.index import RULE, match_ohajimetoshite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class OhajimetoshiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '東京をはじめとして各地で開催する。': 'をはじめとして',
            '日本を始めとして多くの国が参加した。': 'を始めとして',
            '東京をはじめとする都市。': None,
            '東京をはじめ各地で開催する。': None,
            '仕事を始めて帰る。': None,
            '東京をはじめ として各地で開催する。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ohajimetoshite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('東京をはじめとして各地で開催する。', [
            tok('東京', '東京', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('はじめ', 'はじめ', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('各地', '各地', ('名詞',)), tok('で開催する。', 'で', ('助詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'はじめ', 'と', 'し', 'て'])
        words = {'東京': object(), '各地': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をはじめとして')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn('東京', words)
        self.assertIn('各地', words)
