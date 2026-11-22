import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikagitsute.index import RULE as SHORT_RULE
from src.grammar_rules.rules.nikagitsuteha.index import RULE, match_nikagitsuteha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikagitsutehaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '今回に限っては無料です。': 'に限っては',
            '彼に限っては心配ない。': 'に限っては',
            '今日にかぎっては休みだ。': 'にかぎっては',
            '今日に限って雨が降る。': None,
            '時間を限っては話せない。': None,
            '今回に 限っては無料だ。': None,
            '今回に限って は無料だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikagitsuteha(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_overlap_and_identity(self):
        source = sentence('今回に限っては無料になります。', [
            tok('今回', '今回', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('限っ', '限る', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('無料になります。', '無料', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '限っ', 'て', 'は'])
        words = {'今回': object()}
        stage.add_cards(words)
        self.assertIn('今回', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に限っては')
        self.assertEqual(words[SHORT_RULE.expression].sentences[0].surface_form, 'に限って')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
