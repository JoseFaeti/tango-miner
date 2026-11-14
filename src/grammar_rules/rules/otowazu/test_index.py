import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.otowazu.index import RULE, match_otowazu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OtowazuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '年齢を問わず参加できる。': 'を問わず',
            '国籍をとわず募集する。': 'をとわず',
            '昼夜を問わず働く。': 'を問わず',
            '責任を問わずに済ませた。': None,
            '問わず語りに話した。': None,
            '年齢を問わない。': None,
            '年齢を 問わず参加できる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_otowazu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('年齢を問わず参加できる。', [
            tok('年齢', '年齢', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('問わ', '問う', ('動詞',)), tok('ず', 'ず', ('助動詞',)),
            tok('参加できる。', '参加', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '問わ', 'ず'])
        words = {'年齢': object()}
        stage.add_cards(words)
        self.assertIn('年齢', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を問わず')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
