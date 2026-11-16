import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niterashite.index import RULE, match_niterashite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiterashiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'法律に照らして判断する。': 'に照らして',
                 '経験に照らして考える。': 'に照らして',
                 'それに照らして判断する。': 'に照らして',
                 '基準にてらして考える。': 'にてらして',
                 '法律に照らす。': None, '法律に照らした判断。': None,
                 '照らして考える。': None, '状況に 照らして考える。': None,
                 '頭に気づいて': None, '道を照らして歩く。': None,
                 '壁に照らして映す。': 'に照らして'}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niterashite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('法律に照らして判断する。', [
            tok('法律', '法律', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('照らし', '照らす', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('判断する。', '判断する', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '照らし', 'て'])
        words = {'法律': object()}
        stage.add_cards(words)
        self.assertIn('法律', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に照らして')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
