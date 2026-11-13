import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.omegutsute.index import RULE, match_omegutsute
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OmegutsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '問題をめぐって議論する。': 'をめぐって',
            '遺産を巡って争う。': 'を巡って',
            '問題をめぐっての議論。': 'をめぐって',
            '問題をめぐる議論。': None,
            '季節が巡ってきた。': None,
            '問題を めぐって議論する。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_omegutsute(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('問題をめぐって議論する。', [
            tok('問題', '問題', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('めぐっ', '巡る', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('議論する。', '議論', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'めぐっ', 'て'])
        words = {'問題': object()}
        stage.add_cards(words)
        self.assertIn('問題', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をめぐって')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
