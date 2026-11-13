import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nishitagatte.index import RULE, match_nishitagatte
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NishitagatteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '年を取るにしたがって変わる。': 'にしたがって',
            '増えるに従って変わる。': 'に従って',
            '規則に従って行う。': 'に従って',
            '彼にしたがって歩く。': 'にしたがって',
            '規則にしたがい行う。': None,
            '従って行う。': None,
            '規則に 従って行う。': None,
            '規則を従って行う。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nishitagatte(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('年を取るに従って寒くなる。', [
            tok('年', '年', ('名詞',)), tok('を', 'を', ('助詞',)),
            tok('取る', '取る', ('動詞',)), tok('に', 'に', ('助詞',)),
            tok('従っ', '従う', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('寒くなる。', '寒い', ('形容詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '従っ', 'て'])
        words = {'取る': object()}
        stage.add_cards(words)
        self.assertIn('取る', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に従って')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
