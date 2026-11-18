import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ikanotowazu.index import RULE, match_ikanotowazu
from src.grammar_rules.rules.otowazu.index import RULE as SHORT_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class IkanotowazuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '結果のいかんを問わず報告する。': 'いかんを問わず',
            '理由如何を問わず参加できる。': '如何を問わず',
            '結果いかんをとわず実施する。': 'いかんをとわず',
            '年齢を問わず参加できる。': None,
            '遺憾を問わず進む。': None,
            '結果いかんを問わない。': None,
            '結果の いかんを問わず報告する。': None,
            '結果いかんを 問わず報告する。': None,
            '結果いかんを問わずに進む。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ikanotowazu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_overlap(self):
        source = sentence('結果のいかんを問わず報告する。', [
            tok('結果', '結果', ('名詞',)), tok('の', 'の', ('助詞',)),
            tok('いかん', 'いかん', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('問わ', '問う', ('動詞',)), tok('ず', 'ず', ('助動詞',)),
            tok('報告する。', '報告', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['いかん', 'を', '問わ', 'ず'])
        words = {'結果': object()}
        stage.add_cards(words)
        self.assertIn('結果', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'いかんを問わず')
        self.assertIn(SHORT_RULE.expression, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
