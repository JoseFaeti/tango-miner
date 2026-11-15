import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.ro_na.index import RULE, match_ro_na
from src.grammar_rules.rules.na.index import RULE as NA_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class RoNaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'食べろ！': ['食べろ'], '行け！': ['行け'], 'しろ！': [],
                 '来い！': ['来い'], '行くな！': ['な'], '食べるなよ。': ['な'],
                 '高いな。': [], '静かな町。': [], '行くかな。': [], '行く な。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_ro_na(tokens)], expected)

    def test_reservation_and_existing_na_overlap(self):
        command = tok('行け', '行く', ('動詞', '一般', '*', '*', '五段', '命令形'))
        verb = tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '終止形-一般'))
        na = tok('な', 'な', ('助詞', '終助詞'))
        prefix = tok('明日はあの場所へ', '場所', ('名詞',))
        punctuation = tok('。', '。', ('補助記号',))
        sources = [sentence('明日はあの場所へ行け。', [prefix, command, punctuation]),
                   sentence('明日はあの場所へ行くな。', [prefix, verb, na, punctuation])]
        stage = GrammarStep(rules=(RULE, NA_RULE))
        artifact = stage.process(Artifact(sources))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(reserved.data[0].tokens[1].get('grammar_reserved', False))
        self.assertFalse(reserved.data[1].tokens[1].get('grammar_reserved', False))
        self.assertTrue(reserved.data[1].tokens[2].get('grammar_reserved', False))
        self.assertEqual(stage.occurrences[RULE.id].count, 2)
        self.assertEqual(stage.occurrences[NA_RULE.id].count, 1)
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
