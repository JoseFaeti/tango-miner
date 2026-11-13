import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.muke_muki.index import RULE, match_muke_muki
from src.grammar_rules.rules.nimuiteiru_muki.index import RULE as SUITABILITY_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MukeMukiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '初心者向けの本。': '向け', '子供向けだ。': '向け',
            '海外向けに販売する。': '向け', '初心者向きの本。': '向き',
            '子供向きだ。': '向き', '顔を向ける。': None,
            '目標に向けて努力する。': None, '向きを変える。': None,
            '北向きの部屋。': None, '子どもむけの本。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_muke_muki(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_suffix_forms(self):
        for text, parts, expected in [
            ('人むけ', [tok('人', '人', ('名詞',)), tok('むけ', 'むけ', ('接尾辞', '名詞的'))], ['むけ']),
            ('人むき', [tok('人', '人', ('名詞',)), tok('むき', 'むき', ('接尾辞', '名詞的'))], ['むき']),
            ('人 向け', [tok('人', '人', ('名詞',)), tok(' ', ' ', ('空白',)), tok('向け', '向け', ('接尾辞', '名詞的'))], []),
            ('を向け', [tok('を', 'を', ('助詞',)), tok('向け', '向ける', ('動詞',))], []),
        ]:
            with self.subTest(text=text):
                tokens, error = aligned_tokens(text, parts)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_muke_muki(tokens)], expected)

    def test_reservation_identity_and_overlap(self):
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
        self.assertIn(SUITABILITY_RULE.expression, words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
