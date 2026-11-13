import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niwatatsute.index import RULE, match_niwatatsute
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiwatatsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '三年にわたって続いた。': 'にわたって',
            '全国に渡って広がる。': 'に渡って',
            '橋に渡って行く。': 'に渡って',
            '橋を渡って行く。': None,
            '長期間にわたる調査。': None,
            '数年にわたった。': None,
            '三年に わたって続いた。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niwatatsute(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('三年にわたって続いた。', [
            tok('三年', '三年', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('わたっ', 'わたる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('続いた。', '続く', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'わたっ', 'て'])
        words = {'三年': object()}
        stage.add_cards(words)
        self.assertIn('三年', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にわたって')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
