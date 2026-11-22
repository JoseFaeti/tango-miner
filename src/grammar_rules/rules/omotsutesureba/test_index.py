import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.omotsutesureba.index import RULE, match_omotsutesureba
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OmotsutesurebaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼の力をもってすれば簡単だ。': 'をもってすれば',
            '最新技術を以てすれば可能だ。': 'を以てすれば',
            '彼の力を持ってすれば可能だ。': 'を持ってすれば',
            '彼の力をもってしても無理だ。': None,
            '彼の力をもってする。': None,
            '彼の力をもって すれば可能だ。': None,
            '本を持って帰ればいい。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_omotsutesureba(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_prefix_overlap(self):
        source = sentence('力をもってすれば可能だ。', [
            tok('力', '力', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('もっ', 'もつ', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('すれ', 'する', ('動詞',)), tok('ば', 'ば', ('助詞',)),
            tok('可能だ。', '可能', ('名詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['を', 'もっ', 'て', 'すれ', 'ば'])
        words = {'力': object()}
        stage.add_cards(words)
        self.assertIn('力', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'をもってすれば')
        self.assertIn('〜をもって', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
