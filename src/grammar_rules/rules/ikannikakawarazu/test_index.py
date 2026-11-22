import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ikannikakawarazu.index import RULE, match_ikannikakawarazu
from src.grammar_rules.rules.nikakawarazu.index import RULE as SHORT_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class IkannikakawarazuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '結果のいかんにかかわらず報告する。': 'いかんにかかわらず',
            '理由如何に関わらず参加できる。': '如何に関わらず',
            '結果の如何に拘わらず実施する。': '如何に拘わらず',
            '遺憾に関わらず進む。': None,
            '天候にかかわらず開催する。': None,
            '結果いかんを問わず実施する。': None,
            '結果の いかんにかかわらず報告する。': None,
            '結果いかんに 関わらず報告する。': None,
            '結果いかんに関わらずに進む。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ikannikakawarazu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_overlap(self):
        source = sentence('結果のいかんにかかわらず報告する。', [
            tok('結果', '結果', ('名詞',)), tok('の', 'の', ('助詞',)),
            tok('いかん', 'いかん', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('かかわら', 'かかわる', ('動詞',)), tok('ず', 'ず', ('助動詞',)),
            tok('報告する。', '報告', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['の', 'いかん', 'に', 'かかわら', 'ず'])
        words = {'結果': object()}
        stage.add_cards(words)
        self.assertIn('結果', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'いかんにかかわらず')
        self.assertIn(SHORT_RULE.expression, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
