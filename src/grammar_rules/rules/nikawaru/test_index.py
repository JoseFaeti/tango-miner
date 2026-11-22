import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikawaru.index import RULE, match_nikawaru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikawaruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '父に代わる人を探す。': 'に代わる',
            '石油にかわるエネルギー。': 'にかわる',
            '父に代わって出席する。': None,
            '父に代わった人。': None,
            '父が代わる。': None,
            '父に 代わる人。': None,
            '色に変わる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikawaru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('父に代わる人を探しています。', [
            tok('父', '父', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('代わる', '代わる', ('動詞',)), tok('人を探しています。', '人', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '代わる'])
        words = {'父': object()}
        stage.add_cards(words)
        self.assertIn('父', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に代わる')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
