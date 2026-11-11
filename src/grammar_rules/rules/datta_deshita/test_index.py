import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.datta_deshita.index import RULE, match_datta_deshita
from src.grammar_rules.rules.hazu_da.index import RULE as HAZU
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DattaDeshitaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'猫だった。': ['だった'], '学生でした。': ['でした'],
                 '静かだったよ。': ['だった'], '静かでした。': ['でした'],
                 '猫だ。': [], '猫です。': [], '猫だったら行く。': [],
                 '猫だったりする。': [], '食べた。': [], '猫 だった。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_datta_deshita(tokens)], expected)

    def test_reservation_identity_and_overlap(self):
        source = sentence('雨が降るはずだった。', [
            tok('雨が', '雨', ('名詞',)),
            tok('降る', '降る', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('はず', 'はず', ('名詞', '普通名詞', '一般')),
            tok('だっ', 'だ', ('助動詞',)), tok('た', 'た', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(HAZU, RULE))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual(stage.occurrences[HAZU.id].count, 1)
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['はず', 'だっ', 'た'])
        words = {'雨': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'だった')
        self.assertIn('雨', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_incomplete_and_wrong_tokens(self):
        for raw in ([tok('猫', '猫', ('名詞',)), tok('だっ', 'だ', ('名詞',)), tok('た', 'た', ('助動詞',))],
                    [tok('猫', '猫', ('名詞',)), tok('だっ', 'だ', ('助動詞',)), tok('た', 'た', ('名詞',))]):
            tokens, error = aligned_tokens('猫だった', raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_datta_deshita(tokens)), [])
