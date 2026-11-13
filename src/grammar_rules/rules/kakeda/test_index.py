import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kakeda.index import RULE, match_kakeda
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KakedaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'本を読みかけだ。': '読みかけだ', '本を読みかけだった。': '読みかけだった',
                 '本を読みかけです。': '読みかけです', '本を読みかけでした。': '読みかけでした',
                 '書き掛けだ。': '書き掛けだ', '食べかけのパン。': None,
                 '仕事は仕掛けだ。': None, '本を読みかける。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kakeda(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card_evidence(self):
        noun = tok('読みかけ', '読みかけ', ('名詞',))
        copula = tok('だ', 'だ', ('助動詞',))
        tokens, error = aligned_tokens('読みかけ だ', [noun, copula])
        self.assertIsNone(error)
        self.assertEqual(list(match_kakeda(tokens)), [])
        source = sentence('私は本を読みかけだと思います。', [tok('私は本を', '本', ('名詞',)), noun,
                                                      copula, tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読みかけ': object()}
        stage.add_cards(words)
        self.assertIn('読みかけ', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '読みかけだ')


if __name__ == '__main__':
    unittest.main()
