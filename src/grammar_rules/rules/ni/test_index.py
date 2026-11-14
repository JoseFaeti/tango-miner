import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.koto_ni_naru.index import RULE as KOTO_NI_NARU
from src.grammar_rules.rules.ni.index import RULE, match_ni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'三時に行く。': 1, '学校に行く。': 1, '部屋にいる。': 1,
                 '友達に渡す。': 1, '学校に三時に行く。': 2,
                 '「学校」に行く。': 1, '学校 に行く。': 0,
                 'に行く。': 0, '肉を食べる。': 0, '猫がいる。': 0}
        for text, count in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ni(tokens)], ['に'] * count)

    def test_reservation_identity_and_overlap(self):
        source = sentence('学校に行くことになる。', [
            tok('学校', '学校', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('こと', 'こと', ('名詞', '普通名詞', '一般')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('なる', 'なる', ('動詞', '一般', '*', '*', '五段', '終止形-一般')),
            tok('。', '。', ('補助記号', '句点'))])
        stage = GrammarStep(rules=(RULE, KOTO_NI_NARU))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'こと', 'に', 'なる'])
        self.assertEqual(stage.occurrences[RULE.id].count, 2)
        self.assertEqual(stage.occurrences[KOTO_NI_NARU.id].count, 1)
        words = {'学校': object(), '行く': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に')
        self.assertTrue(all(word in words for word in ('学校', '行く')))
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_non_particle_and_punctuation_controls(self):
        for previous, particle in [
            (tok('学校', '学校', ('名詞',)), tok('に', 'に', ('助動詞',))),
            (tok('、', '、', ('補助記号', '読点')), tok('に', 'に', ('助詞', '格助詞'))),
            (tok('学校', '学校', ('名詞',)), tok('に', '違う', ('助詞', '格助詞'))),
        ]:
            text = previous['surface'] + particle['surface']
            tokens, error = aligned_tokens(text, [previous, particle])
            self.assertIsNone(error)
            self.assertEqual(list(match_ni(tokens)), [])
