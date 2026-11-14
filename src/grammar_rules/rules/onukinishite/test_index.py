import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.onukinishite.index import RULE, match_onukinishite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OnukinishiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '冗談を抜きにして話す。': 'を抜きにして',
            '彼をぬきにして考える。': 'をぬきにして',
            '彼を抜きにしては語れない。': 'を抜きにして',
            '歯を抜きにして病院へ行く。': 'を抜きにして',  # ambiguous literal purpose
            '彼を抜きにした会議。': None,
            '彼を抜きにする。': None,
            '冗談抜きにして話す。': None,
            '彼を 抜きにして話す。': None,
            '彼を抜きにしている。': 'を抜きにして',
            '釘を抜いて捨てる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_onukinishite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('冗談を抜きにして話す。', [
            tok('冗談', '冗談', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('抜き', '抜き', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('話す。', '話す', ('動詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '抜き', 'に', 'し', 'て'])
        words = {'冗談': object()}
        stage.add_cards(words)
        self.assertIn('冗談', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を抜きにして')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
