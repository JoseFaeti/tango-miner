import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.zumogana.index import RULE, match_zumogana


class ZumoganaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '言わずもがなのことを言った。': 'ずもがな',
            '言わずもがなだ。': 'ずもがな',
            'せずもがなだった。': 'ずもがな',
            '見ずもがな。': 'ずもがな',
            '知らずもがな。': 'ずもがな',
            'ずもがな。': None,
            '言わずに済む。': None,
            '言わず もがな。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_zumogana(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card(self):
        parts = [tok('言わ', '言う', ('動詞', '一般', '*', '*', '*', '未然形-一般')),
                 tok('ず', 'ず', ('助動詞',)), tok('も', 'も', ('助詞',)),
                 tok('がな', 'がな', ('名詞',)), tok('の', 'の', ('助詞',)),
                 tok('こと。', 'こと', ('名詞',))]
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([sentence('言わずもがなのこと。', parts)])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ず', 'も', 'がな'])
        words = {'言う': object()}
        stage.add_cards(words)
        self.assertIn('言う', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ずもがな')
