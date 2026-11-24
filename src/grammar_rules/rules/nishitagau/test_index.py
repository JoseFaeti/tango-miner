import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nishitagau.index import RULE, match_nishitagau
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NishitagauTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '規則に従う。': 'に従う',
            '指示にしたがう。': 'にしたがう',
            '彼に従う。': 'に従う',
            'それに従う。': 'に従う',
            '規則に従う人。': 'に従う',
            '規則に従う': 'に従う',
            '規則にしたがう。': 'にしたがう',
            '規則に従うべきだ。': 'に従う',
            '規則に従うと決めた。': 'に従う',
            '規則に従った。': None,
            '規則に従わない。': None,
            '規則に従います。': None,
            '規則に従えばいい。': None,
            '規則に従って行動する。': None,
            '規則に従い行動する。': None,
            '彼を従う。': None,
            '従う人。': None,
            '規則、に従う。': None,
            '規則に、従う。': None,
            '規則に 従う。': None,
            '規則 に従う。': None,
            '年を取るに従う。': None,
            '従うという言葉。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nishitagau(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('私たちは学校の規則に従う。', [
            tok('私たち', '私たち', ('代名詞',)), tok('は', 'は', ('助詞',)),
            tok('学校', '学校', ('名詞',)), tok('の', 'の', ('助詞',)),
            tok('規則', '規則', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('従う', '従う', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '従う'])
        words = {'規則': object()}
        stage.add_cards(words)
        self.assertIn('規則', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に従う')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
