import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nari.index import RULE, match_nari
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NariTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は部屋に入るなり、倒れた。': ['なり'],
            '彼は帰るなり寝た。': ['なり'],
            '見るなり逃げた。': ['なり'],
            '食べるなり飲むなりしなさい。': [],
            '好きなりに考える。': [], '彼は大人になりました。': [],
            '行くなり。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nari(aligned)], expected)

    def test_card_reserves_marker_and_keeps_verb(self):
        source = sentence('彼は家に帰るなり寝た。', [
            tok('彼は家に', '家', ('名詞',)),
            tok('帰る', '帰る', ('動詞', '一般', '*', '*', '五段-ラ行', '終止形-一般')),
            tok('なり', 'なり', ('助詞', '接続助詞')),
            tok('寝た。', '寝る', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = Artifact([source, source])
        stage.process(artifact)
        words = {'帰る': object()}
        stage.add_cards(words)
        self.assertIn('帰る', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'なり')
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([bool(t.get('grammar_reserved')) for t in artifact.data[0].tokens],
                         [False, False, True, False])


if __name__ == '__main__':
    unittest.main()
