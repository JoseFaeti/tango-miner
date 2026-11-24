import unittest

from sudachipy import dictionary, tokenizer

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nikanshiteno.index import RULE, match_nikanshiteno
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NikanshitenoTests(unittest.TestCase):
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
        positives = ['契約に関しての説明を読む。',
                     'それに関しての説明があります。',
                     '二件に関しての報告です。']
        negatives = ['契約に関して説明します。', '契約については説明します。',
                     '問題に関する本。', '事件に関係しては困る。',
                     '問題に関しましての説明です。', '問題に関しても説明します。',
                     'に関しての説明します。', '契約に、関しては説明します。']
        for text in positives + negatives:
            with self.subTest(text=text):
                self.assertEqual(
                    [text[a:b] for a, b in match_nikanshiteno(self.tokens(text))],
                    ['に関しての'] if text in positives else [])

    def test_alignment_gaps(self):
        tokens = self.tokens('契約に関しての説明です。')
        for index in range(1, 5):
            altered = [dict(t) for t in tokens]
            a, b = altered[index]['span']
            altered[index]['span'] = (a + 1, b)
            self.assertEqual(list(match_nikanshiteno(altered)), [])

    def test_card_reservation_and_attached_word(self):
        text = 'それに関しての説明があります。'
        source = sentence(text, self.tokens(text))
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens
                          if t.get('grammar_reserved')][:4], ['に', '関し', 'て', 'の'])
        self.assertFalse(data.data[0].tokens[0].get('grammar_reserved'))
        words = {'それ': object()}
        stage.add_cards(words)
        self.assertIn('それ', words)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に関しての')
        self.assertEqual(stage.occurrences['nikanshite_nikansuru'].count, 2)
