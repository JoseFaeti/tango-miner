import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.ozenteini.index import RULE, match_ozenteini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OzenteiniTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '成功を前提に計画する。': 'を前提に',
            '合格することを前提にしている。': 'を前提に',
            '結婚を前提にした交際。': 'を前提に',
            'それを前提にする。': 'を前提に',
            '前提に問題がある。': None,
            '成功を前提としている。': None,
            '成功を前提条件にする。': None,
            '成功を 前提にする。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ozenteini(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_lexical_candidate(self):
        source = sentence('成功を前提に計画する。', [
            tok('成功', '成功', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('前提', '前提', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('計画', '計画', ('名詞',)), tok('する', 'する', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '前提', 'に'])
        words = {'成功': object()}
        stage.add_cards(words)
        self.assertIn('成功', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を前提に')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
