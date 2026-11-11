import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.da_desu.index import RULE, match_da_desu
from src.grammar_rules.rules.hazu_da.index import RULE as HAZU
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class DaDesuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'猫だ。': ['だ'], '学生です。': ['です'], '静かだよ。': ['だ'],
                 '高いです。': ['です'], '猫だった。': [], '猫でした。': [],
                 '猫ではない。': [], '猫なら行く。': [], '出す。': [], '猫 です。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_da_desu(tokens)], expected)

    def test_reservation_identity_and_lexical_evidence(self):
        source = sentence('これはかわいい猫です。', [tok('これ', 'これ', ('代名詞',)),
                          tok('は', 'は', ('助詞',)), tok('かわいい', 'かわいい', ('形容詞',)), tok('猫', '猫', ('名詞',)),
                          tok('です', 'です', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')], ['です'])
        words = {'猫': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'です')
        self.assertIn('猫', words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)

    def test_longer_grammar_overlap(self):
        source = sentence('明日は雨が降るはずだよ。', [
            tok('明日は雨が', '雨', ('名詞',)),
            tok('降る', '降る', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('はず', 'はず', ('名詞', '普通名詞', '一般')), tok('だ', 'だ', ('助動詞',)),
            tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep(rules=(HAZU, RULE))
        stage.process(Artifact([source]))
        self.assertEqual(stage.occurrences[HAZU.id].count, 1)
        self.assertEqual(stage.occurrences[RULE.id].count, 1)

    def test_wrong_part_of_speech(self):
        tokens, error = aligned_tokens('猫だ', [tok('猫', '猫', ('名詞',)), tok('だ', 'だ', ('名詞',))])
        self.assertIsNone(error)
        self.assertEqual(list(match_da_desu(tokens)), [])
