import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.niataisuru.index import RULE, match_niataisuru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiataisuruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '評価に値する。': ['に値する'], '読むに値した。': ['に値した'],
            '注目に値しない。': ['に値しない'], '注目に値しなかった。': ['に値しなかった'],
            '賞賛に値します。': ['に値します'], '賞賛に値しません。': ['に値しません'],
            '議論に値している。': ['に値して'], '評価に値すれば。': ['に値すれば'],
            '百万円に値する。': ['に値する'], '値を計算する。': [],
            '評価に 値する。': [], '評価の値する。': [], '評価に値段がある。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niataisuru(tokens)], expected)

    def test_reservation_and_identity(self):
        source = sentence('これは評価に値する作品だと思う。', [tok('これは', 'これ', ('名詞',)), tok('評価', '評価', ('名詞',)),
                          tok('に', 'に', ('助詞',)), tok('値', '値', ('名詞',)),
                          tok('する', 'する', ('動詞',)), tok('作品だと思う。', '作品', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '値', 'する'])
        words = {'評価': object()}
        stage.add_cards(words)
        self.assertIn('評価', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に値する')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
