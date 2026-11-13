import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niawasete.index import RULE, match_niawasete
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiawaseteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'予定に合わせて出発する。': 'に合わせて',
                 'これに合わせて変える。': 'に合わせて',
                 '季節にあわせて服を選ぶ。': 'にあわせて',
                 '予定に合わせまして変更します。': 'に合わせまして',
                 '手を合わせて祈る。': None,
                 '予定に合わせる。': None,
                 '予定に 合わせて出発する。': None,
                 '時計に合わせて針を動かす。': 'に合わせて'}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niawasete(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('予定に合わせて出発しましょう。', [
            tok('予定', '予定', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('合わせ', '合わせる', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('出発しましょう。', '出発', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '合わせ', 'て'])
        words = {'予定': object()}
        stage.add_cards(words)
        self.assertIn('予定', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に合わせて')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
