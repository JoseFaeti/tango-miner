import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nisuginai.index import RULE, match_nisuginai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NisuginaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'学生にすぎない。': 'にすぎない', '事実に過ぎなかった。': 'に過ぎなかった',
                 '行くにすぎません。': 'にすぎません', 'それにすぎなくて': 'にすぎなくて',
                 '学生にすぎなければ': 'にすぎなければ', '学生にすぎませんでした': 'にすぎませんでした',
                 '時間が過ぎない': None, '多すぎない': None, '学生にすぎる': None,
                 'にすぎない': None, '学生に すぎない': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nisuginai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼は学生にすぎないと思う。', [
            tok('彼は', '彼', ('名詞',)),
            tok('学生', '学生', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('すぎ', 'すぎる', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
            tok('と思う。', '思う', ('動詞',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'すぎ', 'ない'])
        words = {'学生': object()}
        stage.add_cards(words)
        self.assertIn('学生', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にすぎない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
