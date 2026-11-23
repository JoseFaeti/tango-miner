import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tokangaerareru.index import RULE, match_tokangaerareru
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class TokangaerareruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '事故が原因だと考えられる。': 'と考えられる',
            '被害が拡大したと考えられた。': 'と考えられた',
            '今後増えると考えられます。': 'と考えられます',
            '彼が犯人と考えられました。': 'と考えられました',
            '彼が来ると 考えられる。': None,
            '彼が来ると考える。': None,
            '彼が来ると考えられない。': None,
            '彼と考えられる人が話す。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tokangaerareru(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼は明日来ると考えられる。', [
            tok('彼は明日', '彼', ('名詞',)),
            tok('来る', '来る', ('動詞', '*', '*', '*', '*', '終止形-一般')),
            tok('と', 'と', ('助詞', '格助詞')), tok('考え', '考える', ('動詞',)),
            tok('られる', 'られる', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '考え', 'られる'])
        words = {'来る': object()}
        stage.add_cards(words)
        self.assertIn('来る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と考えられる')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
