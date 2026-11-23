import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tosareru.index import RULE, match_tosareru
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class TosareruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '必要とされる人のみが永遠の命をあたえられる世が！': None,
            '必要とされた。': None,
            '必要とされます。': None,
            '必要とされている。': None,
            '禁断の古代呪法のために、たくさんの魂を必要とされている。': None,
            '勇者のベルトとされる。': 'とされる',
            '村を守るとされる。': 'とされる',
            '問題とされる。': 'とされる',
            '必要だとされる。': 'とされる',
            '彼は天才とされる。': 'とされる',
            '原因は事故だとされた。': 'とされた',
            '彼は犯人とされました。': 'とされました',
            '重要とされている。': 'とされて',
            '彼とされる。': 'とされる',
            '本を読むとされる。': 'とされる',
            '彼に質問をされる。': None,
            '彼が来るとする。': None,
            '彼は敵とされない。': None,
            '彼は天才と される。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tosareru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_predicate(self):
        source = sentence('彼は昔から天才とされる。', [
            tok('彼は昔から', '彼', ('名詞',)), tok('天才', '天才', ('名詞',)),
            tok('と', 'と', ('助詞', '格助詞')), tok('さ', 'する', ('動詞',)),
            tok('れる', 'れる', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'さ', 'れる'])
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'とされる')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
