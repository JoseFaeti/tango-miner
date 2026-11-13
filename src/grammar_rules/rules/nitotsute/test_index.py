import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitotsute.index import RULE, match_nitotsute
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NitotsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'私にとって大切だ。': 'にとって', '彼にとっての問題だ。': 'にとって',
                 '子供にとっては難しい。': 'にとって', '私にとりまして大切です。': 'にとりまして',
                 '手に取って見る。': None, '棚に取って置く。': None,
                 '私に とって大切だ。': None, '本をとって読む。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitotsute(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('私にとって大切な本です。', [
            tok('私', '私', ('代名詞',)), tok('に', 'に', ('助詞',)),
            tok('とっ', 'とる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('大切な本です。', '大切', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'とっ', 'て'])
        words = {'私': object()}
        stage.add_cards(words)
        self.assertIn('私', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にとって')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
