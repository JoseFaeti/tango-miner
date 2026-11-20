import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ntobakarini.index import RULE, match_ntobakarini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NtobakariniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '言わんとばかりに睨んだ。': ['んとばかりに'],
            '泣かんとばかりに訴えた。': ['んとばかりに'],
            '壊れんとばかりに揺れた。': ['んとばかりに'],
            '来んとばかりに走った。': ['んとばかりに'],
            '本とばかりに書いた。': [],
            '読んとばかりにした。': [],
            '言わんばかりに睨んだ。': [],
            '言わんとばかりだ。': [],
            '言わんとばかり に睨んだ。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ntobakarini(tokens)], expected, raw)

    def test_identity_reservation_and_lexical_verb(self):
        source = sentence('言わんとばかりに睨んだ。', [
            tok('言わ', '言う', ('動詞', '一般', '*', '*', '五段-ワア行', '未然形-一般')),
            tok('ん', 'ぬ', ('助動詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('ばかり', 'ばかり', ('助詞', '副助詞')), tok('に', 'に', ('助詞', '格助詞')),
            tok('睨んだ。', '睨む', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ん', 'と', 'ばかり', 'に'])
        words = {}
        stage.add_cards(words)
        self.assertIn(RULE.expression, words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'んとばかりに')
        self.assertNotIn('〜ぬ / 〜ん', words)
        self.assertNotIn('〜ばかり', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_merged_and_sahen_tokens(self):
        cases = [
            ('壊れんとばかりに', [tok('壊れん', '壊れる',
             ('動詞', '一般', '*', '*', '下一段-ラ行', '連体形-撥音便'))]),
            ('攻撃せんとばかりに', [tok('攻撃', '攻撃', ('名詞', '普通名詞', 'サ変可能')),
                                  tok('せん', 'せん', ('名詞', '普通名詞'))]),
        ]
        for text, prefix in cases:
            with self.subTest(text=text):
                raw = prefix + [tok('と', 'と', ('助詞', '格助詞')),
                                tok('ばかり', 'ばかり', ('助詞', '副助詞')),
                                tok('に', 'に', ('助詞', '格助詞'))]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ntobakarini(tokens)], ['んとばかりに'])
