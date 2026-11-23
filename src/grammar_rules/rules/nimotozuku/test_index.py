import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nimotozuite.index import match_nimotozuite
from src.grammar_rules.rules.nimotozuku.index import RULE, match_nimotozuku
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NimotozukuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'事実に基づく判断。': 'に基づく',
                 '事実に基づいた計画。': 'に基づいた',
                 'それに基づく。': 'に基づく',
                 '規則にもとづいた計画。': 'にもとづいた',
                 '事実に基づいて判断する。': None,
                 '基づく判断。': None, '規則に 基づく。': None,
                 '頭に気づく。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nimotozuku(tokens)],
                                 [] if expected is None else [expected])
                if '基づいて' in text:
                    self.assertEqual([text[a:b] for a, b in match_nimotozuite(tokens)], ['に基づいて'])

    def test_reservation_and_identity(self):
        source = sentence('事実に基づいた計画。', [
            tok('事実', '事実', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('基づい', '基づく', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('計画', '計画', ('名詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '基づい', 'た'])
        words = {'事実': object()}
        stage.add_cards(words)
        self.assertIn('事実', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に基づいた')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
