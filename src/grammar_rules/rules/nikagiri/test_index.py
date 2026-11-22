import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikagiri.index import RULE, match_nikagiri
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikagiriTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本日に限り休業します。': 'に限り',
            '会員に限り無料です。': 'に限り',
            '今日にかぎり、休み。': 'にかぎり',
            '会員に限ります。': None,
            '今日に限って雨だ。': None,
            '子供に限らず大人も楽しめる。': None,
            '人間に限りなく近い。': None,
            '可能な限り努力する。': None,
            '会員に 限り無料です。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikagiri(tokens)],
                                 [] if expected is None else [expected], tokens)

    def test_reservation_and_identity(self):
        source = sentence('会員に限り無料です。', [
            tok('会員', '会員', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('限り', '限る', ('動詞',)), tok('無料です。', '無料', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '限り'])
        words = {'会員': object()}
        stage.add_cards(words)
        self.assertIn('会員', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に限り')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
