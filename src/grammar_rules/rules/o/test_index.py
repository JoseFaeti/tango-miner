import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.o.index import RULE, match_o
from src.grammar_rules.rules.onukinishite.index import RULE as ONUKINISHITE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class OTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'本を読む。': 1, '公園を歩く。': 1, '家を出る。': 1,
                 '本を読み、道を歩く。': 2, '「本」を読む。': 1,
                 '猫がいる。': 0, '本 を読む。': 0, 'を読む。': 0}
        for text, count in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_o(tokens)], ['を'] * count)

    def test_reservation_identity_and_overlap(self):
        source = sentence('彼を抜きにして本を読む。', [
            tok('彼', '彼', ('代名詞',)), tok('を', 'を', ('助詞', '格助詞')),
            tok('抜き', '抜き', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('本', '本', ('名詞',)), tok('を', 'を', ('助詞', '格助詞')),
            tok('読む', '読む', ('動詞',)), tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE, ONUKINISHITE))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['を', '抜き', 'に', 'し', 'て', 'を'])
        self.assertEqual(stage.occurrences[RULE.id].count, 2)
        self.assertEqual(stage.occurrences[ONUKINISHITE.id].count, 1)
        words = {'彼': object(), '本': object(), '読む': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'を')
        self.assertTrue(all(word in words for word in ('彼', '本', '読む')))
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_non_particle_and_punctuation_controls(self):
        for previous, particle in [
            (tok('本', '本', ('名詞',)), tok('を', 'を', ('名詞',))),
            (tok('、', '、', ('補助記号', '読点')), tok('を', 'を', ('助詞', '格助詞'))),
            (tok('本', '本', ('名詞',)), tok('を', '違う', ('助詞', '格助詞'))),
        ]:
            text = previous['surface'] + particle['surface']
            tokens, error = aligned_tokens(text, [previous, particle])
            self.assertIsNone(error)
            self.assertEqual(list(match_o(tokens)), [])


if __name__ == '__main__':
    unittest.main()
