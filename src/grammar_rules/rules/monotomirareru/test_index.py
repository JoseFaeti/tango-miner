import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.monotomirareru.index import RULE, match_monotomirareru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class MonotomirareruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '事故が原因であるものと見られる。': 'ものと見られる',
            '被害は拡大したものと見られた。': 'ものと見られた',
            '今後増えるものと見られます。': 'ものと見られます',
            '事故が原因とされたものと見られました。': 'ものと見られました',
            '彼が買ったものと見られる。': 'ものと見られる',
            '私のものと見られる。': None,
            '事故が原因である物と見られる。': None,
            '事故が原因である ものと見られる。': None,
            '事故が原因であるものと 見られる。': None,
            '事故が原因であるものと見る。': None,
            '事故が原因であるものと見られない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_monotomirareru(tokens)],
                                 [] if expected is None else [expected])

    def test_components_and_identity(self):
        source = sentence('来るものと見られる。', [
            tok('来る', '来る', ('動詞', '*', '*', '*', '*', '連体形-一般')),
            tok('もの', 'もの', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('見', '見る', ('動詞',)), tok('られる', 'られる', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(data.data[0].tokens[0].get('grammar_reserved'))
        self.assertTrue(all(t.get('grammar_reserved') for t in data.data[0].tokens[1:5]))
        words = {'来る': object(), 'もの': object()}
        stage.add_cards(words)
        self.assertIn('来る', words)
        self.assertIn('もの', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ものと見られる')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
