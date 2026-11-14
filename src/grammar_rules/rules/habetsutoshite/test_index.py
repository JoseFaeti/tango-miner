import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.habetsutoshite.index import RULE, match_habetsutoshite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class HabetsutoshiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '値段は別として品質はいい。': 'は別として',
            '結果はべつとして進む。': 'はべつとして',
            'できるかどうかは別として試そう。': 'は別として',
            '彼は別としてもいい。': 'は別として',  # literal separation is ambiguous
            '別として扱う。': None,
            'それは、別として扱う。': None,
            'それは 別として扱う。': None,
            'それは別とした問題だ。': None,
            'それは別の問題だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_habetsutoshite(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_topic_overlap(self):
        source = sentence('値段は別として品質はいい。', [
            tok('値段', '値段', ('名詞',)), tok('は', 'は', ('助詞', '係助詞')),
            tok('別', '別', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('品質はいい。', '品質', ('名詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['は', '別', 'と', 'し', 'て'])
        words = {'値段': object()}
        stage.add_cards(words)
        self.assertIn('値段', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'は別として')
        self.assertIn('〜は', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
