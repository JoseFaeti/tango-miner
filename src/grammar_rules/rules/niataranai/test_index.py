import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niataranai.index import RULE, match_niataranai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiataranaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '驚くに当たらない。': 'に当たらない',
            '非難に当たりません。': 'に当たりません',
            '批判に当たらなかった。': 'に当たらなかった',
            '違反に当たりませんでした。': 'に当たりませんでした',
            '驚くにあたらない。': 'にあたらない',
            '壁に当たらない。': 'に当たらない',
            '非難に当たらなくてもよい。': 'に当たらなく',
            '違反に当たらなければよい。': 'に当たらなければ',
            '壁に当たる。': None, '壁には当たらない。': None,
            '壁に 当たらない。': None, '壁に当たらなかっ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niataranai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('これは違反に当たらない。', [
            tok('これは', 'これ', ('代名詞',)), tok('違反', '違反', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('当たら', '当たる', ('動詞',)),
            tok('ない', 'ない', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '当たら', 'ない'])
        words = {'違反': object()}
        stage.add_cards(words)
        self.assertIn('違反', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に当たらない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
