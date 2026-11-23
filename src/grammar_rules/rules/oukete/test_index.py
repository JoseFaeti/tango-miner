import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.oukete.index import RULE, match_oukete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class OuketeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'結果を受けて判断する。': 'を受けて',
                 'それを受けて考える。': 'を受けて',
                 '報告をうけて決める。': 'をうけて',
                 '結果を受けた計画。': None, '結果を受ける。': None,
                 '受けて考える。': None, '結果を 受けて考える。': None,
                 'ボールを受けて投げる。': 'を受けて', '試験を受ける。': None,
                 '仕事を受け取って帰る。': None, '結果を受け取って判断する。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_oukete(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('結果を受けて判断する。', [
            tok('結果', '結果', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('受け', '受ける', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('判断する。', '判断する', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '受け', 'て'])
        words = {'結果': object()}
        stage.add_cards(words)
        self.assertIn('結果', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を受けて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
