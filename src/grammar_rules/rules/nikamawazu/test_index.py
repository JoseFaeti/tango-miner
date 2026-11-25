import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikamawazu.index import RULE, match_nikamawazu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NikamawazuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '人目にかまわず泣いた。': 'にかまわず',
            '雨に構わず出かけた。': 'に構わず',
            '人目にかまわずに泣いた。': 'にかまわず',
            '濡れるのにかまわず走った。': 'にかまわず',
            '服装にはかまわず出かけた。': None,
            '誰もかまわず出かけた。': None,
            '雨に構わない。': None,
            '天候にかかわらず開催する。': None,
            '人目に かまわず泣いた。': None,
            '人目 にかまわず泣いた。': None,
            'にかまわず': None,
            '。にかまわず': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikamawazu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('天候にかまわず開催する。', [
            tok('天候', '天候', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('かまわ', 'かまう', ('動詞',)), tok('ず', 'ず', ('助動詞',)),
            tok('開催する。', '開催', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'かまわ', 'ず'])
        words = {'天候': object()}
        stage.add_cards(words)
        self.assertIn('天候', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にかまわず')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)

