import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitodomaranai.index import RULE, match_nitodomaranai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NitodomaranaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '影響は日本にとどまらない。': 'にとどまらない',
            '人気は国内に留まらない。': 'に留まらない',
            '被害は家屋に止まらない。': 'に止まらない',
            '影響はこれにとどまらない。': 'にとどまらない',
            '国内にとどまる。': None,
            '国内にとどまらず広がる。': None,
            '被害は国内にとどまらなかった。': 'にとどまらなかった',
            '影響は国内にとどまりません。': 'にとどまりません',
            '影響は国内に とどまらない。': None,
            '列車が止まらない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitodomaranai(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('日本にとどまらない。', [
            tok('日本', '日本', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('とどまら', 'とどまる', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'とどまら', 'ない'])
        words = {'日本': object()}
        stage.add_cards(words)
        self.assertIn('日本', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にとどまらない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
