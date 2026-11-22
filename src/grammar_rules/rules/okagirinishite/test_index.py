import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.okagirinishite.index import RULE, match_okagirinishite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OkagirinishiteTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '今日を限りにして仕事を辞める。': 'を限りにして',
            '今回をかぎりにして引退する。': 'をかぎりにして',
            '今日を限りに仕事を辞める。': None,
            '今日に限りにして辞める。': None,
            '今日を限りに して辞める。': None,
            '今日を限りにした。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_okagirinishite(tokens)],
                                 [] if expected is None else [expected], tokens)

    def test_reservation_overlap_and_identity(self):
        source = sentence('今日を限りにして引退する。', [
            tok('今日', '今日', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('限り', '限り', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('引退する。', '引退', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '限り', 'に', 'し', 'て'])
        words = {'今日': object()}
        stage.add_cards(words)
        self.assertIn('今日', words)
        self.assertIn('〜を限りに', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を限りにして')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
