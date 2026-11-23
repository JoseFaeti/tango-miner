import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.okagirini.index import RULE, match_okagirini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OkagiriniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '今日を限りに仕事を辞める。': 'を限りに',
            '今回を限りに引退する。': 'を限りに',
            '今年をかぎりに店を閉める。': 'をかぎりに',
            '時間を限る。': None,
            '限りなく広い。': None,
            '今日に限り休業する。': None,
            '今日を 限りに辞める。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_okagirini(tokens)],
                                 [] if expected is None else [expected], tokens)

    def test_reservation_and_identity(self):
        source = sentence('今日を限りに引退する。', [
            tok('今日', '今日', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('限り', '限り', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('引退する。', '引退', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '限り', 'に'])
        words = {'今日': object()}
        stage.add_cards(words)
        self.assertIn('今日', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を限りに')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
