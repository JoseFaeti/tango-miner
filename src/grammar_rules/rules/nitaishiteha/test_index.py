import unittest

from sudachipy import dictionary, tokenizer

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nitaishiteha.index import RULE, match_nitaishiteha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NitaishitehaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.analyzer = dictionary.Dictionary(
            config_path='sudachi.config.json', dict='full').create()

    def tokens(self, text):
        raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
               for m in self.analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
        tokens, error = aligned_tokens(text, raw)
        self.assertIsNone(error)
        return tokens

    def test_forms_and_controls(self):
        cases = {
            '先生に対しては失礼だ。': 'に対しては',
            'これに対しては反対だ。': 'に対しては',
            '兄は静かなのに対しては、弟は賑やかだ。': 'に対しては',
            '問題に対しましては後で答えます。': 'に対しましては',
            '先生に対して話す。': None,
            '先生に対しまして話す。': None,
            '問題に対する答え。': None,
            '敵と対しては戦えない。': None,
            'に対しては反対だ。': None,
            '問題に 対しては答えない。': None,
            '問題 に対しては答えない。': None,
            '問題に対して は答えない。': None,
            '問題に対した。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                self.assertEqual([text[a:b] for a, b in match_nitaishiteha(self.tokens(text))],
                                 [expected] if expected else [])

    def test_tail_variants_and_boundaries(self):
        for polite in (False, True):
            for merged in (False, True):
                raw = [tok('問題', '問題', ('名詞',)), tok('に', 'に', ('助詞',)),
                       tok('対し', '対する', ('動詞',))]
                if polite:
                    raw.append(tok('まし', 'ます', ('助動詞',)))
                raw += ([tok('ては', 'ては', ('助詞', '接続助詞'))] if merged else
                        [tok('て', 'て', ('助詞', '接続助詞')),
                         tok('は', 'は', ('助詞', '係助詞'))])
                text = ''.join(t['surface'] for t in raw)
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual(list(match_nitaishiteha(tokens)), [(2, len(text))])
                for i in range(1, len(tokens)):
                    altered = [dict(t) for t in tokens]
                    a, b = altered[i]['span']
                    altered[i]['span'] = (a + 1, b)
                    self.assertEqual(list(match_nitaishiteha(altered)), [])
                if not merged:
                    tokens[-1]['pos'] = ['名詞']
                    self.assertEqual(list(match_nitaishiteha(tokens)), [])

    def test_identity_reservation_and_base_recall(self):
        text = 'これに対しては反対だ。'
        source = sentence(text, self.tokens(text))
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertFalse(data.data[0].tokens[0].get('grammar_reserved'))
        self.assertEqual(''.join(t['surface'] for t in data.data[0].tokens
                                 if t.get('grammar_reserved') and t['surface'] != 'だ'), 'に対しては')
        words = {'これ': object()}
        stage.add_cards(words)
        self.assertIn('これ', words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に対しては')
        # The existing shorter grammar identity remains independently useful.
        self.assertEqual(stage.occurrences['nitaishite'].count, 2)
