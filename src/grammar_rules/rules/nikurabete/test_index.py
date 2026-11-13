import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikurabete.index import RULE, match_nikurabete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikurabeteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'去年に比べて暑い。': 'に比べて',
                 'これに比べては小さい。': 'に比べて',
                 '兄にくらべて背が高い。': 'にくらべて',
                 '昨年に比べまして増えた。': 'に比べまして',
                 '兄と比べて背が高い。': None,
                 '写真を比べてみる。': None,
                 '去年に比べると暑い。': None,
                 '去年に 比べて暑い。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikurabete(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('去年に比べて今年はずっと暑いですね。', [
            tok('去年', '去年', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('比べ', '比べる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('今年はずっと暑いですね。', '今年', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '比べ', 'て'])
        words = {'去年': object()}
        stage.add_cards(words)
        self.assertIn('去年', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に比べて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
