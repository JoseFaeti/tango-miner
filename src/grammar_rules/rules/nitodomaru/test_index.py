import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitodomaru.index import RULE, match_nitodomaru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NitodomaruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '被害は一部にとどまる。': 'にとどまる',
            '影響は国内に留まった。': 'に留まった',
            '参加者は十人に止まります。': 'に止まります',
            '被害はこれにとどまる。': 'にとどまる',
            '国内にとどまらず世界に広がる。': None,
            '国内にとどまらない。': None,
            '国内に とどまる。': None,
            '列車が止まる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitodomaru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('被害は一部にとどまる。', [
            tok('被害は', '被害', ('名詞',)), tok('一部', '一部', ('名詞',)),
            tok('に', 'に', ('助詞',)), tok('とどまる', 'とどまる', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'とどまる'])
        words = {'一部': object()}
        stage.add_cards(words)
        self.assertIn('一部', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にとどまる')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
