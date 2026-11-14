import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.naikotoniha.index import RULE, match_naikotoniha
from src.grammar_rules.rules.nai.index import RULE as NAI_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NaikotonihaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '実際にやってみないことには分からない。': 'ないことには',
            '雨が降らないことには困る。': 'ないことには',
            '安くないことには買えない。': 'ないことには',
            '元気でないことには働けない。': 'ないことには',
            'お金がないことには始まらない。': 'ないことには',
            '行かないことはない。': None,
            '行くことには意味がある。': None,
            'ないことにする。': None,
            '行かないことに は困る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naikotoniha(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_overlapping_negative(self):
        source = sentence('行かないことには分からない。', [
            tok('行か', '行く', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞', '*', '*', '*', '助動詞-ナイ', '連体形-一般')),
            tok('こと', 'こと', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('分からない。', '分かる', ('動詞',))])
        stage = GrammarStep(rules=(RULE, NAI_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', 'こと', 'に', 'は'])
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ないことには')
        self.assertEqual(words[NAI_RULE.expression].frequency, 1)
        from src.GrammarConstants import GRAMMAR_EXPRESSIONS
        from src.grammar_rules.registry import RULES
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertIn(RULE, RULES)
