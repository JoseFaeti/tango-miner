import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikagitsute.index import RULE, match_nikagitsute
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikagitsuteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '今日に限って雨が降る。': 'に限って',
            '彼に限ってそんなことはしない。': 'に限って',
            '今回に限って無料です。': 'に限って',
            '今日にかぎって雨だ。': 'にかぎって',
            '会員に限る。': None,
            '子供に限らず大人も楽しめる。': None,
            '時間を限って話す。': None,
            '今日に 限って雨だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikagitsute(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('今日に限って雨が降っています。', [
            tok('今日', '今日', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('限っ', '限る', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('雨が降っています。', '雨', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '限っ', 'て'])
        words = {'今日': object()}
        stage.add_cards(words)
        self.assertIn('今日', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に限って')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
