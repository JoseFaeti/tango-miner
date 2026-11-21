import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.tosareteiru.index import RULE, match_tosareteiru
from src.grammar_rules.rules.tosareru.index import RULE as BASE
from src.grammar_rules.rules.te_iru.index import RULE as ASPECT
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TosareteiruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は天才とされている。': 'とされている',
            '原因は事故だとされていた。': 'とされていた',
            '重要とされています。': 'とされています',
            '重要とされていない。': 'とされていない',
            '本を読むとされている。': 'とされている',
            '彼に質問をされている。': None,
            '彼は天才とされる。': None,
            '彼は天才とされて、帰った。': None,
            '彼は天才と されている。': None,
            '彼は天才とされて いる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tosareteiru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_overlap(self):
        source = sentence('彼は天才とされている。', [
            tok('彼は', '彼', ('名詞',)), tok('天才', '天才', ('名詞',)),
            tok('と', 'と', ('助詞', '格助詞')), tok('さ', 'する', ('動詞',)),
            tok('れ', 'れる', ('助動詞', '*', '*', '*', '*', '連用形')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('いる', 'いる', ('動詞', '非自立可能')), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, BASE, ASPECT), prefer_expressions=False)
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'さ', 'れ', 'て', 'いる'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とされている')
        self.assertIn(BASE.expression, words)
        self.assertIn(ASPECT.expression, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
