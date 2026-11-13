import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nimuiteiru_muki.index import RULE, match_nimuiteiru_muki
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NimuiteiruMukiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '仕事に向いている。': 'に向いている', '仕事に向いています。': 'に向いています',
            '仕事に向いてない。': 'に向いてない', '仕事に向いてた。': 'に向いてた',
            '初心者向きの本。': '向き', '子供向きだ。': '向き',
            '駅に向いている。': 'に向いている',
            '北向きの部屋。': None, '顔を向いている。': None,
            '仕事に向かっている。': None, '仕事に向いて歩く。': None,
            '仕事に 向いている。': None, '向きを変える。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nimuiteiru_muki(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_source(self):
        source = sentence('初心者向きの本を読みます。', [
            tok('初心者', '初心者', ('名詞',)), tok('向き', '向き', ('接尾辞', '名詞的')),
            tok('の本を読みます。', '本', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['向き'])
        words = {'初心者': object()}
        stage.add_cards(words)
        self.assertIn('初心者', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '向き')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
