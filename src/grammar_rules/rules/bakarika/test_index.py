import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.bakarika.index import RULE, match_bakarika
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class BakarikaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'本ばかりか雑誌も読む。': 'ばかりか',
                 '読むばかりか書くこともできる。': 'ばかりか',
                 '高いばかりか不便だ。': 'ばかりか',
                 '読んだばかりか書いた。': 'ばかりか',
                 '本ばかりか。': None, '本ばかり読む。': None,
                 '本 ばかりか雑誌も読む。': None,
                 '本ばかり か雑誌も読む。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_bakarika(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_past_overlap(self):
        source = sentence('読んだばかりか書いた。', [
            tok('読ん', '読む', ('動詞',)), tok('だ', 'だ', ('助動詞',)),
            tok('ばかり', 'ばかり', ('助詞', '副助詞')),
            tok('か', 'か', ('助詞', '副助詞')),
            tok('書いた。', '書く', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertTrue(all(t.get('grammar_reserved') for t in data.data[0].tokens
                            if t['surface'] in ('ばかり', 'か')))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ばかりか')
        self.assertNotIn('〜ばかり', words)
        self.assertNotIn('〜たばかり', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
