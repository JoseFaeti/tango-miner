import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitodomarazu.index import RULE, match_nitodomarazu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NitodomarazuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '日本にとどまらず世界に広がった。': 'にとどまらず',
            '国内に留まらず海外でも人気だ。': 'に留まらず',
            '被害は家屋に止まらず周囲にも及んだ。': 'に止まらず',
            'これにとどまらず他にもある。': 'にとどまらず',
            '国内にとどまる。': None,
            '国内にとどまらない。': None,
            '国内に とどまらず広がった。': None,
            '列車が止まらず通過した。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nitodomarazu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('日本にとどまらず世界に広がった。', [
            tok('日本', '日本', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('とどまら', 'とどまる', ('動詞',)), tok('ず', 'ず', ('助動詞',)),
            tok('世界に広がった。', '世界', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'とどまら', 'ず'])
        words = {'日本': object()}
        stage.add_cards(words)
        self.assertIn('日本', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にとどまらず')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
