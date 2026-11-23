import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nihaataranai.index import RULE, match_nihaataranai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NihaataranaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '驚くには当たらない。': 'には当たらない',
            '非難には当たりません。': 'には当たりません',
            '批判には当たらなかった。': 'には当たらなかった',
            '違反には当たりませんでした。': 'には当たりませんでした',
            '驚くにはあたらない。': 'にはあたらない',
            '壁には当たらない。': 'には当たらない',
            '非難には当たらなくてもよい。': 'には当たらなく',
            '違反には当たらなければよい。': 'には当たらなければ',
            '壁には当たる。': None, '壁に当たらない。': None,
            '壁には 当たらない。': None, '壁には当たらなかっ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nihaataranai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('驚くには当たらない。', [
            tok('驚く', '驚く', ('動詞',)), tok('に', 'に', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('当たら', '当たる', ('動詞',)),
            tok('ない', 'ない', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'は', '当たら', 'ない'])
        words = {'驚く': object()}
        stage.add_cards(words)
        self.assertIn('驚く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'には当たらない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
