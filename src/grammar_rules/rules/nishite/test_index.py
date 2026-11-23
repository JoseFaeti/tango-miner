import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nishite.index import RULE, match_nishite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NishiteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'彼は三十歳にして成功した。': True,
                 '十年にして初めて分かった。': True,
                 '一瞬にして消えた。': True,
                 '子供にしては背が高い。': False,
                 '十年にしても長い。': False,
                 '彼を代表にして働く。': False,
                 '三十歳に して成功した。': False}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nishite(tokens)],
                                 ['にして'] if expected else [])

    def test_reservation_and_identity(self):
        source = sentence('十年にして初めて分かった。', [
            tok('十', '十', ('名詞', '数詞')), tok('年', '年', ('接尾辞',)),
            tok('に', 'に', ('助詞',)), tok('し', 'する', ('動詞',)),
            tok('て', 'て', ('助詞',)), tok('初めて', '初めて', ('副詞',)),
            tok('分かっ', '分かる', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'し', 'て', '初めて'])
        words = {'年': object()}
        stage.add_cards(words)
        self.assertIn('年', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にして')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
