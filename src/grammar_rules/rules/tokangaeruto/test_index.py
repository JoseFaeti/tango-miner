import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tokangaeruto.index import RULE, match_tokangaeruto
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class TokangaerutoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼が来ると考えると嬉しい。': 'と考えると',
            '事故が原因だと考えると納得できる。': 'と考えると',
            '彼が帰ったと考えると寂しい。': 'と考えると',
            '彼と考えると答えが出る。': None,
            '将来のことを考えると不安だ。': None,
            '彼が来ると考える。': None,
            '彼が来ると考えられる。': None,
            '彼が来ると 考えると嬉しい。': None,
            '彼が来ると考える と嬉しい。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tokangaeruto(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼が来ると考えると嬉しい。', [
            tok('彼が', '彼', ('名詞',)),
            tok('来る', '来る', ('動詞', '*', '*', '*', '*', '終止形-一般')),
            tok('と', 'と', ('助詞', '格助詞')), tok('考える', '考える', ('動詞',)),
            tok('と', 'と', ('助詞', '接続助詞')), tok('嬉しい。', '嬉しい', ('形容詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '考える', 'と'])
        words = {'来る': object()}
        stage.add_cards(words)
        self.assertIn('来る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と考えると')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
