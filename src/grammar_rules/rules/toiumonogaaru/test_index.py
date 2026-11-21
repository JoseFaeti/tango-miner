import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.mono_ga_aru.index import match_mono_ga_aru
from src.grammar_rules.rules.toiu.index import match_toiu
from src.grammar_rules.rules.toiumonogaaru.index import RULE, match_toiumonogaaru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToiumonogaaruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '日本には花見というものがある。': 'というものがある',
            '昔、花見というものがあった。': 'というものがあった',
            '花見というものがあります。': 'というものがあります',
            '花見というものがありました。': 'というものがありました',
            '花見っていうものがある。': 'っていうものがある',
            '花見と言うものがある。': 'と言うものがある',
            '花見というものがない。': None,
            '猫というものがいる。': None,
            '美しいものがある。': None,
            '花見というものを知る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiumonogaaru(tokens)],
                                 [] if expected is None else [expected])
                if expected:
                    self.assertEqual(list(match_toiu(tokens)), [])
                    self.assertEqual(list(match_mono_ga_aru(tokens)), [])
        text = '美しいものがある。'
        raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
               for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
        tokens, _ = aligned_tokens(text, raw)
        self.assertTrue(list(match_mono_ga_aru(tokens)))

    def test_identity_and_reservation(self):
        tokens = [tok('花見', '花見', ('名詞',)), tok('と', 'と', ('助詞',)),
                  tok('いう', 'いう', ('動詞',)), tok('もの', 'もの', ('名詞',)),
                  tok('が', 'が', ('助詞',)), tok('ある', 'ある', ('動詞',)),
                  tok('。', '。', ('補助記号',))]
        source = sentence('花見というものがある。', tokens)
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertTrue(all(t.get('grammar_reserved') for t in reserved.data[0].tokens[1:6]))
        self.assertFalse(reserved.data[0].tokens[0].get('grammar_reserved'))
        words = {'もの': object()}
        stage.add_cards(words)
        self.assertIn(RULE, RULES)
        self.assertIn('もの', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'というものがある')
