import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.okitsukakeni.index import RULE, match_okitsukakeni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OkitsukakeniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'留学をきっかけに日本語を学んだ。': 'をきっかけに',
                 '出会いを切っ掛けにして始めた。': 'を切っ掛けに',
                 '病気をきっかけにした話。': 'をきっかけに',
                 'それをきっかけにする。': 'をきっかけに',
                 'きっかけになる。': None, '事故をきっかけとして変わった。': None,
                 '留学を きっかけに学んだ。': None, 'きっかけを探す。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_okitsukakeni(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_lexical_candidate(self):
        source = sentence('留学をきっかけに学びました。', [
            tok('留学', '留学', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('きっかけ', 'きっかけ', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('学びました。', '学ぶ', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'きっかけ', 'に'])
        words = {'留学': object()}
        stage.add_cards(words)
        self.assertIn('留学', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をきっかけに')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
