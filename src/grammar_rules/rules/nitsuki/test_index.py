import unittest

from sudachipy import dictionary, tokenizer

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nitsuki.index import RULE, match_nitsuki
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NitsukiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.analyzer = dictionary.Dictionary(
            config_path='sudachi.config.json', dict='full').create()

    def tokens(self, text):
        raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
               for m in self.analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
        aligned, error = aligned_tokens(text, raw)
        self.assertIsNone(error)
        return aligned

    def test_forms_and_lexical_controls(self):
        cases = {
            '雨天につき中止します。': 'につき',
            '工事中に付き通行止め。': 'に付き',
            '一人につき二枚です。': 'につき',
            '一枚につき百円です。': 'につき',
            '事件につき説明する。': 'につき',
            '机に付きます。': None, '仕事につきました。': None,
            '壁に付きながら歩く。': None, '仕事につきまして説明します。': None,
            '席について話す。': None, '駅に着き、休む。': None,
            '月に一度休む。': None, 'につき中止。': None,
            '雨天に、つき中止。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual([text[a:b] for a, b in match_nitsuki(self.tokens(text))],
                                 [] if expected is None else [expected])

    def test_boundaries(self):
        tokens = self.tokens('雨天につき中止。')
        tokens[2]['span'] = (4, 6)
        self.assertEqual(list(match_nitsuki(tokens)), [])

    def test_card_reservation_and_attached_word(self):
        text = '雨天につき中止します。'
        source = sentence(text, self.tokens(text))
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens
                          if t.get('grammar_reserved')], ['に', 'つき'])
        words = {'雨天': object()}
        stage.add_cards(words)
        self.assertIn('雨天', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'につき')
