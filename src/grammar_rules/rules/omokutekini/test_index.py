import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.omokutekini.index import RULE, match_omokutekini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OmokutekiniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'留学を目的に日本語を学ぶ。': 'を目的に',
                 '調査を目的にして訪れた。': 'を目的に',
                 '交流を目的にした会。': 'を目的に',
                 'それを目的にする。': 'を目的に',
                 '目的になる。': None, '目的を探す。': None,
                 '調査を目的として訪れた。': None, '調査を 目的にする。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_omokutekini(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_lexical_candidate(self):
        source = sentence('留学を目的に学びました。', [
            tok('留学', '留学', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('目的', '目的', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('学びました。', '学ぶ', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '目的', 'に'])
        words = {'留学': object()}
        stage.add_cards(words)
        self.assertIn('留学', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を目的に')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
