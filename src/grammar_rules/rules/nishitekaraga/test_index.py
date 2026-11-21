import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nishitekaraga.index import RULE, match_nishitekaraga
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NishitekaragaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'先生にしてからが間違えている。': 'にしてからが',
                 '私にしてからが知らない。': 'にしてからが',
                 '先生にしてから、話す。': None,
                 '先生にしてから が困る。': None,
                 'にしてからが困る。': None,
                 '仕事をしてから学校へ行く。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nishitekaraga(tokens)],
                                 [expected] if expected else [])

    def test_reservation_and_identity(self):
        source = sentence('先生にしてからが間違う。', [
            tok('先生', '先生', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('から', 'から', ('助詞',)), tok('が', 'が', ('助詞',)),
            tok('間違う', '間違う', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'し', 'て', 'から', 'が'])
        words = {'先生': object()}
        stage.add_cards(words)
        self.assertIn('先生', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にしてからが')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
