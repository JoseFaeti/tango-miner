import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.okaerimizu.index import RULE, match_okaerimizu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OkaerimizuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        from sudachipy import dictionary, tokenizer
        analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        cases = {
            '危険を顧みず進む。': 'を顧みず',
            '家族を顧みずに働く。': 'を顧みずに',
            '過去をかえりみず進む。': 'をかえりみず',
            '過去を省みず進む。': None,
            '危険を顧みない。': None,
            '顧みず進む。': None,
            '危険を 顧みず進む。': None,
            '後ろを振り返らず進む。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_okaerimizu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('危険を顧みずに進む。', [
            tok('危険', '危険', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('顧み', '顧みる', ('動詞',)), tok('ず', 'ず', ('助動詞',)),
            tok('に', 'に', ('助詞',)), tok('進む。', '進む', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '顧み', 'ず', 'に'])
        words = {'危険': object()}
        stage.add_cards(words)
        self.assertIn('危険', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を顧みずに')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
