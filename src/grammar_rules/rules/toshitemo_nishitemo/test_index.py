import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nishitemo.index import RULE as NI_RULE
from src.grammar_rules.rules.toshitemo.index import RULE as TO_RULE
from src.grammar_rules.rules.toshitemo_nishitemo.index import RULE, match_toshitemo_nishitemo
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToshitemoNishitemoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降るとしても出かける。': ['としても'],
            '行ったにしても遅すぎる。': ['にしても'],
            '代表としても働く。': ['としても'],
            '雨にしても雪にしても出かける。': ['にしても', 'にしても'],
            '仕事をしても終わらない。': [],
            '子供に しても無理だ。': [],
            '子供にしては背が高い。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toshitemo_nishitemo(tokens)], expected)

    def test_identity_reservation_and_overlapping_cards(self):
        source = sentence('雨にしても出かける。', [
            tok('雨', '雨', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('も', 'も', ('助詞',)), tok('出かける', '出かける', ('動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, NI_RULE, TO_RULE))
        artifact = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'し', 'て', 'も'])
        words = {'雨': object()}
        stage.add_cards(words)
        self.assertIn('雨', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にしても')
        self.assertIn(NI_RULE.expression, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
