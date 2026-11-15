import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.darouni.index import RULE, match_darouni
from src.grammar_rules.rules.darou_deshou.index import RULE as DAROU_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DarouniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '早く言えばよかっただろうに。': 'だろうに',
            '彼も喜ぶだろうに、残念だ。': 'だろうに',
            '一緒に行けば楽しいでしょうに。': 'でしょうに',
            '彼は学生だろう。': None,
            '行くだろうか。': None,
            '太郎に会う。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_darouni(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_overlap(self):
        copula = tok('だろう', 'だ', ('助動詞',))
        ni = tok('に', 'に', ('助詞', '接続助詞'))
        source = sentence('彼もきっと喜ぶだろうに。', [tok('彼もきっと', '彼', ('名詞',)), tok('喜ぶ', '喜ぶ', ('動詞',)), copula, ni,
                                      tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, DAROU_RULE))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens
                          if t.get('grammar_reserved')], ['だろう', 'に'])
        words = {'喜ぶ': object()}
        stage.add_cards(words)
        self.assertIn('喜ぶ', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'だろうに')
        self.assertIn(DAROU_RULE.expression, words)
        aligned, error = aligned_tokens('だろう に', [copula, ni])
        self.assertIsNone(error)
        self.assertEqual(list(match_darouni(aligned)), [])
