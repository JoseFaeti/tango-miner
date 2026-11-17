import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikamakete.index import RULE, match_nikamakete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikamaketeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'仕事にかまけて家族を顧みない。': 'にかまけて',
                 'それにかまけて勉強を怠る。': 'にかまけて',
                 '仕事にかまける。': None, '仕事にかまけた。': None,
                 'かまけている。': None, '仕事に かまけている。': None,
                 '仕事に構えている。': None, '仕事をかまけている。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikamakete(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('仕事にかまけて家族を顧みない。', [
            tok('仕事', '仕事', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('かまけ', 'かまける', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('家族', '家族', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('顧みない。', '顧みる', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'かまけ', 'て'])
        words = {'仕事': object()}
        stage.add_cards(words)
        self.assertIn('仕事', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にかまけて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
