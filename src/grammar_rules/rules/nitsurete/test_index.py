import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitsurete.index import RULE, match_nitsurete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NitsureteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '年を取るにつれて寒くなる。': 'につれて',
            '時代の変化につれて変わる。': 'につれて',
            '増えるにつれて変わる。': 'につれて',
            '子供を学校に連れて行く。': None,
            '年齢に連れて変わる。': None,
            '子供をつれて行く。': None,
            '年を取るにつれ変わる。': None,
            '年を取るに つれて変わる。': None,
            '子供を学校につれて行く。': 'につれて',
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitsurete(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('年を取るにつれて寒くなる。', [
            tok('年', '年', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('取る', '取る', ('動詞',)), tok('に', 'に', ('助詞',)),
            tok('つれ', 'つれる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('寒くなる。', '寒い', ('形容詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'つれ', 'て'])
        words = {'取る': object()}
        stage.add_cards(words)
        self.assertIn('取る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'につれて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
