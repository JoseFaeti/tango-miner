import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiwazaruoenai.index import RULE, match_toiwazaruoenai
from src.grammar_rules.rules.zaruoenai.index import RULE as GENERAL_RULE


class ToiwazaruoenaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '失敗と言わざるを得ない。': 'と言わざるを得ない',
            '失敗といわざるをえなかった。': 'といわざるをえなかった',
            '失敗と言わざるを得ません。': 'と言わざるを得ません',
            '失敗と言わざるを得ず。': 'と言わざるを得ず',
            '言わざるを得ない。': None, '失敗と考えざるを得ない。': None,
            '失敗と言わざるを得た。': None, '失敗と、言わざるを得ない。': None,
            '失敗と言わざるを得なかっ。': None, 'と言わざるを得ない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toiwazaruoenai(aligned)],
                                 [] if expected is None else [expected])

    def test_identity_reservation_and_general_rule_overlap(self):
        parts = [tok('失敗', '失敗', ('名詞',)), tok('と', 'と', ('助詞',)),
                 tok('言わ', '言う', ('動詞',)), tok('ざる', 'ぬ', ('助動詞',)),
                 tok('を', 'を', ('助詞',)), tok('得', '得る', ('動詞',)),
                 tok('ない', 'ない', ('助動詞',))]
        source = sentence('失敗と言わざるを得ない', parts)
        stage = GrammarStep(rules=(RULE, GENERAL_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        self.assertTrue(all(t.get('grammar_reserved') for t in result.data[0].tokens[1:]))
        words = {}
        stage.add_cards(words)
        self.assertIn(RULE, RULES)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'と言わざるを得ない')
        self.assertEqual(words[GENERAL_RULE.expression].sentences[0].surface_form, 'ざるを得ない')
