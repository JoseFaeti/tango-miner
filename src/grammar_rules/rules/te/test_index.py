import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te.index import match_te, RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TeCardTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'食べて帰る。': ['て'], '読んで寝る。': ['で'],
                 '早くて便利だ。': ['て'], '読んで。': ['で'],
                 '食べている。': ['て'], '学校で読む。': [], '手を洗う。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te(tokens)], expected)

    def test_boundaries_and_card_integration(self):
        verb = tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便'))
        particle = tok('で', 'で', ('助詞', '接続助詞'))
        source = sentence('私は本を読んでから寝ることにした。', [
            tok('私は本を', '本', ('名詞',)), verb, particle,
            tok('から寝ることにした。', 'から', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        stage.process(Artifact([source, source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'で')
        tokens, error = aligned_tokens('読ん で', [verb, particle])
        self.assertIsNone(error)
        self.assertEqual(list(match_te(tokens)), [])


if __name__ == '__main__':
    unittest.main()
