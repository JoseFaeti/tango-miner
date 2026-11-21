import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.monotoomowareru.index import RULE, match_monotoomowareru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MonotoomowareruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '事故が原因であるものと思われる。': 'ものと思われる',
            '被害は拡大したものと思われた。': 'ものと思われた',
            '今後増えるものと思われます。': 'ものと思われます',
            '事故が原因とされたものと思われました。': 'ものと思われました',
            '彼が買ったものと思われる。': 'ものと思われる',
            '私のものと思われる。': None,
            '事故が原因である物と思われる。': None,
            '事故が原因である ものと思われる。': None,
            '事故が原因であるものと 思われる。': None,
            '事故が原因であるものと思う。': None,
            '事故が原因であるものと思われない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_monotoomowareru(tokens)],
                                 [] if expected is None else [expected])

    def test_components_and_identity(self):
        source = sentence('来るものと思われる。', [
            tok('来る', '来る', ('動詞', '*', '*', '*', '*', '連体形-一般')),
            tok('もの', 'もの', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('思わ', '思う', ('動詞',)), tok('れる', 'れる', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(data.data[0].tokens[0].get('grammar_reserved'))
        self.assertTrue(all(t.get('grammar_reserved') for t in data.data[0].tokens[1:5]))
        words = {'来る': object(), 'もの': object()}
        stage.add_cards(words)
        self.assertIn('来る', words)
        self.assertIn('もの', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ものと思われる')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
