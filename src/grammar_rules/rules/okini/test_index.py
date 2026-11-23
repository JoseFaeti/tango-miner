import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.okini.index import RULE, match_okini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OkiniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'留学を機に日本語を学んだ。': 'を機に',
                 '出会いを機にして始めた。': 'を機に',
                 '病気を機にした話。': 'を機に',
                 'それを機にする。': 'を機に',
                 '機になる。': None, '事故を機として変わった。': None,
                 '留学を 機に学んだ。': None, '機を探す。': None, '飛行機を機に近づけた。': 'を機に',
                 '危機に直面する。': None, '機に乗る。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_okini(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_lexical_candidate(self):
        source = sentence('留学を機に学びました。', [
            tok('留学', '留学', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('機', '機', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('学びました。', '学ぶ', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '機', 'に'])
        words = {'留学': object()}
        stage.add_cards(words)
        self.assertIn('留学', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を機に')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
