import unittest

from sudachipy import dictionary, tokenizer

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nitsukimashiteha.index import RULE, match_nitsukimashiteha
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NitsukimashitehaTests(unittest.TestCase):
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
        positives = ['契約につきましては、後日説明します。',
                     'それにつきましては問題ありません。',
                     '二件につきましては確認中です。']
        negatives = ['契約につきまして説明します。', '契約については説明します。',
                     '雨天につき中止します。', '席に付きましては話し始めた。',
                     '駅に着きましては休む。', '契約につきました。',
                     'につきましては説明します。', '契約に、つきましては説明します。']
        for text in positives + negatives:
            with self.subTest(text=text):
                self.assertEqual(
                    [text[a:b] for a, b in match_nitsukimashiteha(self.tokens(text))],
                    ['につきましては'] if text in positives else [])

    def test_tail_tokenizations_and_gaps(self):
        for text, expected_tail in [('契約につきましては、後日説明します。', ['ては']),
                                    ('それにつきましては問題ありません。', ['て', 'は'])]:
            tokens = self.tokens(text)
            start = next(i for i, t in enumerate(tokens) if t['surface'] == 'まし') + 1
            self.assertEqual([t['surface'] for t in tokens[start:start + len(expected_tail)]],
                             expected_tail)
            for index in range(1, start + len(expected_tail)):
                altered = [dict(t) for t in tokens]
                a, b = altered[index]['span']
                altered[index]['span'] = (a + 1, b)
                self.assertEqual(list(match_nitsukimashiteha(altered)), [])

    def test_card_reservation_and_attached_word(self):
        text = 'それにつきましては問題ありません。'
        source = sentence(text, self.tokens(text))
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens
                          if t.get('grammar_reserved')][:5], ['に', 'つき', 'まし', 'て', 'は'])
        self.assertFalse(data.data[0].tokens[0].get('grammar_reserved'))
        words = {'それ': object()}
        stage.add_cards(words)
        self.assertIn('それ', words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'につきましては')
        self.assertEqual(stage.occurrences['nitsuki'].count, 0)
        self.assertEqual(stage.occurrences['nitsuite'].count, 2)
