import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niitatsute.index import RULE as NIITATSUTE
from src.grammar_rules.rules.niitatsuteha.index import RULE, match_niitatsuteha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiitatsutehaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼に至っては名前も知らない。': 'に至っては',
            '今に至っては遅い。': 'に至っては',
            '駅に至っては休んだ。': 'に至っては',
            '事態に至って気づいた。': None,
            '至って静かだ。': None,
            '彼へ至っては遅い。': None,
            '彼に 至っては遅い。': None,
            '彼に至って は遅い。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niitatsuteha(tokens)],
                                 [] if expected is None else [expected])

    def test_kana_and_particle_controls(self):
        text = '彼にいたっては'
        raw = [tok('彼', '彼', ('代名詞',)), tok('に', 'に', ('助詞',)),
               tok('いたっ', 'いたる', ('動詞',)), tok('て', 'て', ('助詞',)),
               tok('は', 'は', ('助詞',))]
        tokens, error = aligned_tokens(text, raw)
        self.assertIsNone(error)
        self.assertEqual([text[a:b] for a, b in match_niitatsuteha(tokens)], ['にいたっては'])
        tokens[-1]['pos'] = ['名詞']
        self.assertEqual(list(match_niitatsuteha(tokens)), [])

    def test_reservation_identity_and_overlap(self):
        source = sentence('彼に至っては知らない。', [
            tok('彼', '彼', ('代名詞',)), tok('に', 'に', ('助詞',)),
            tok('至っ', '至る', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('知らない。', '知る', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '至っ', 'て', 'は'])
        words = {'彼': object()}
        stage.add_cards(words)
        self.assertIn('彼', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に至っては')
        self.assertNotIn(NIITATSUTE.expression, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
