import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nbakarini.index import RULE, match_nbakarini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NbakariniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '泣かんばかりに訴えた。': ['んばかりに'],
            '飛び出さんばかりに驚いた。': ['んばかりに'],
            '壊れんばかりに揺れた。': ['んばかりに'],
            '来んばかりに走った。': ['んばかりに'],
            '本ばかりに注目した。': [],
            '読んばかりにした。': [],
            '言わんとばかりに睨んだ。': [],
            '泣かんばかりだ。': [],
            '泣かんばかり に訴えた。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nbakarini(tokens)], expected, raw)

    def test_reservation_identity_and_lexical_verb(self):
        sources = [sentence('泣かんばかりに訴えた。', [
            tok('泣か', '泣く', ('動詞', '一般', '*', '*', '五段-カ行', '未然形-一般')),
            tok('ん', 'む', ('助動詞',)), tok('ばかり', 'ばかり', ('助詞', '副助詞')),
            tok('に', 'に', ('助詞', '格助詞')), tok('訴えた。', '訴える', ('動詞',))]),
            sentence('壊れんばかりに揺れた。', [
                tok('壊れん', '壊れる', ('動詞', '一般', '*', '*', '下一段-ラ行', '連体形-撥音便')),
                tok('ばかり', 'ばかり', ('助詞', '副助詞')),
                tok('に', 'に', ('助詞', '格助詞')), tok('揺れた。', '揺れる', ('動詞',))])]
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact(sources)))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['ん', 'ばかり', 'に'])
        self.assertFalse(data.data[1].tokens[0].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'んばかりに')
        self.assertNotIn('〜ばかり', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_volitional_auxiliary_is_not_negative(self):
        source = sentence('泣かんばかりに訴えた。', [
            tok('泣か', '泣く', ('動詞', '一般', '*', '*', '五段-カ行', '未然形-一般')),
            tok('ん', 'ぬ', ('助動詞',)), tok('ばかり', 'ばかり', ('助詞', '副助詞')),
            tok('に', 'に', ('助詞', '格助詞')), tok('訴えた。', '訴える', ('動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {}
        stage.add_cards(words)
        self.assertIn(RULE.expression, words)
        self.assertNotIn('〜ぬ / 〜ん', words)
