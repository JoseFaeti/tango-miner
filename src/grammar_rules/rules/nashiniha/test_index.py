import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nashini_nashideha.index import RULE as SHORT_RULE
from src.grammar_rules.rules.nashiniha.index import RULE, match_nashiniha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NashinihaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '努力なしには成功しない。': 'なしには',
            '彼の支えなしにはここまで来られなかった。': 'なしには',
            '努力することなしには合格できない。': 'なしには',
            '努力無しには実現できない。': '無しには',
            '彼なしには決められない。': 'なしには',
            '梨には触れられない。': None,
            'なしには決められない。': None,
            '努力 なしには成功しない。': None,
            '努力なし に は成功しない。': None,
            '努力なしに は成功しない。': None,
            '努力なしで は成功しない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nashiniha(aligned)],
                                 [expected] if expected else [])

    def test_registered_identity_and_overlap_reservation(self):
        self.assertIn(RULE, RULES)
        parts = [tok('努力', '努力', ('名詞',)), tok('なし', 'なし', ('名詞',)),
                 tok('に', 'に', ('助詞',)), tok('は', 'は', ('助詞',)),
                 tok('成功', '成功', ('名詞',)), tok('しない', 'する', ('動詞',))]
        stage = GrammarStep(rules=(SHORT_RULE, RULE))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('努力なしには成功しない', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['なし', 'に', 'は'])
        words = {'努力': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'なしには')
        self.assertEqual(words[SHORT_RULE.expression].sentences[0].surface_form, 'なしに')
        self.assertIn('努力', words)
