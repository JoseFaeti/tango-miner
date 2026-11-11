import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, TABAKARI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.bakari.index import RULE, match_bakari
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class BakariTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'肉ばかり食べる。': ['ばかり'], '遊んでばかりいる。': ['ばかり'],
                 '文句ばっかり言う。': ['ばっかり'], '酒ばっか飲む。': ['ばっか'],
                 '今食べたばかりだ。': [], '嘘をついたばかりに困った。': [],
                 '本 ばかり読む。': [], '馬鹿を見る。': []}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_bakari(tokens)], expected, raw)

    def test_reservation_identity_and_recent_precedence(self):
        source = sentence('私は毎日肉ばかり食べています。', [
            tok('私は毎日', '毎日', ('名詞',)), tok('肉', '肉', ('名詞',)),
            tok('ばかり', 'ばかり', ('助詞', '副助詞')),
            tok('食べています。', '食べる', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['ばかり'])
        words = {'肉': object()}
        stage.add_cards(words)
        self.assertIn('肉', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ばかり')
        self.assertNotIn(TABAKARI_EXPRESSION, words)
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
