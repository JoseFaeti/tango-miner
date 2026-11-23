import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niitarumade.index import RULE, match_niitarumade
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NiitarumadeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'子供から大人に至るまで楽しめる。': 'に至るまで',
                 '細部にいたるまで調べた。': 'にいたるまで',
                 '駅に至るまで歩く。': 'に至るまで',
                 '結論に至る。': None, '駅まで歩く。': None,
                 '駅に 至るまで歩く。': None, '駅へ至るまで歩く。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niitarumade(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('子供から大人に至るまで楽しめる。', [
            tok('子供から', '子供', ('名詞',)), tok('大人', '大人', ('名詞',)),
            tok('に', 'に', ('助詞', '格助詞')), tok('至る', '至る', ('動詞',)),
            tok('まで', 'まで', ('助詞', '副助詞')), tok('楽しめる。', '楽しむ', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '至る', 'まで'])
        words = {'大人': object()}
        stage.add_cards(words)
        self.assertIn('大人', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に至るまで')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
