import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ohaikeini.index import RULE, match_ohaikeini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OhaikeiniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'不況を背景に失業率が上がった。': 'を背景に',
                 '山を背景にして写真を撮った。': 'を背景に',
                 '戦争を背景にした物語。': 'を背景に',
                 'それを背景にする。': 'を背景に',
                 '背景になる。': None, '事故を背景として変わった。': None,
                 '不況を 背景に増えた。': None, '背景を探す。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ohaikeini(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_lexical_candidate(self):
        source = sentence('留学を背景に学びました。', [
            tok('留学', '留学', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('背景', '背景', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('学びました。', '学ぶ', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '背景', 'に'])
        words = {'留学': object()}
        stage.add_cards(words)
        self.assertIn('留学', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を背景に')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
