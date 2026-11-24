import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikakawatsute.index import RULE, match_nikakawatsute
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NikakawatsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '事件に関わっている。': 'に関わって',
            '教育にかかわって働く。': 'にかかわって',
            '仕事に拘わってきた。': 'に拘わって',
            'それに関わって話す。': 'に関わって',
            '命に関わる問題だ。': None, '仕事に関わった。': None,
            '年齢に関わらず参加できる。': None,
            '雨にもかかわらず出かけた。': None,
            '仕事にも関わっている。': None,
            '仕事に 関わっている。': None, '仕事 に関わっている。': None,
            '仕事に関わっ ている。': None, '事件が関わっている。': None,
            '関わっている。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikakawatsute(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('命に関わっている問題です。', [
            tok('命', '命', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('関わっ', '関わる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('いる問題です。', 'いる', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '関わっ', 'て'])
        words = {'命': object()}
        stage.add_cards(words)
        self.assertIn('命', words)
        self.assertNotIn('〜に関わる', words)
        self.assertFalse(data.data[0].tokens[0].get('grammar_reserved', False))
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に関わって')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
