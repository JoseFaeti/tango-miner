import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nihaoyobanai.index import RULE, match_nihaoyobanai
from src.grammar_rules.rules.nioyobanai.index import match_nioyobanai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NihaoyobanaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '心配するには及ばない。': 'には及ばない',
            '心配には及びません。': 'には及びません',
            '実力は彼には及ばなかった。': 'には及ばなかった',
            '彼には及びませんでした。': 'には及びませんでした',
            '心配するにはおよばない。': 'にはおよばない',
            '彼には及ばなくても頑張る。': 'には及ばなく',
            '彼には及ばなければ困る。': 'には及ばなければ',
            '被害は全国には及ぶ。': None, '彼に及ばない。': None,
            '彼には 及ばない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nihaoyobanai(tokens)],
                                 [] if expected is None else [expected])
                if expected:
                    self.assertEqual(list(match_nioyobanai(tokens)), [])

    def test_reservation_and_identity(self):
        source = sentence('心配するには及ばない。', [
            tok('心配', '心配', ('名詞',)), tok('する', 'する', ('動詞',)),
            tok('に', 'に', ('助詞',)), tok('は', 'は', ('助詞',)),
            tok('及ば', '及ぶ', ('動詞',)), tok('ない', 'ない', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'は', '及ば', 'ない'])
        words = {'心配': object()}
        stage.add_cards(words)
        self.assertIn('心配', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'には及ばない')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
