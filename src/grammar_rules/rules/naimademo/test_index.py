import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nai.index import RULE as NAI_RULE
from src.grammar_rules.rules.naimademo.index import RULE, match_naimademo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NaimademoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '毎日とは言わないまでも、週に一度は運動する。': ['ないまでも'],
            '完璧ではないまでも、十分な出来だ。': ['ないまでも'],
            '高くないまでも安くはない。': ['ないまでも'],
            'お金がないまでも、時間はある。': ['ないまでも'],
            '危ないまでも進む。': [],
            '行くまでもない。': [],
            '行かないまでだ。': [],
            '行かない までもいい。': [],
            '行かないまで もいい。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naimademo(aligned)], expected, raw)

    def test_reservation_identity_and_negative_overlap(self):
        source = sentence('言わないまでも認める。', [
            tok('言わ', '言う', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞', '*', '*', '*', '助動詞-ナイ', '連体形-一般')),
            tok('まで', 'まで', ('助詞',)), tok('も', 'も', ('助詞',)),
            tok('認める。', '認める', ('動詞',))])
        stage = GrammarStep(rules=(RULE, NAI_RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', 'まで', 'も'])
        self.assertFalse(result.data[0].tokens[0].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ないまでも')
        self.assertIn(NAI_RULE.expression, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
