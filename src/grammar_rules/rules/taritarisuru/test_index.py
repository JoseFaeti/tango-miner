import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import TARITARISURU_EXPRESSION as EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.taritarisuru.index import match_taritarisuru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.steps.AddWordsToAnkiStep.index import export_words_to_anki
from src.steps.AddWordsToAnkiStep.test_index import note


class TaritarisuruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '読んだり書いたりする。': 'たりする',
            '本を読んだりする。': 'だりする',
            '食べたりします。': 'たりします',
            '行ったり来たりした。': 'たりした',
            '高かったり安かったりする。': 'たりする',
            '静かだったりする。': 'たりする',
            '寝てたりする。': 'たりする',
            '二人だったりする。': 'たりする',
            '足りない。': None, '二人でする。': None,
            '食べたり、飲んだり。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_taritarisuru(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_incomplete_forms(self):
        verb = tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '連用形-一般'))
        particle = tok('たり', 'たり', ('助詞', '副助詞'))
        suru = tok('し', 'する', ('動詞',))
        for raw in ([verb, particle, suru],
                    [verb, tok('たり', 'たり', ('名詞',)), tok('する', 'する', ('動詞',))],
                    [tok('食べる', '食べる', ('動詞', '一般', '*', '*', '下一段', '終止形')), particle,
                     tok('する', 'する', ('動詞',))]):
            text = ''.join(t['surface'] for t in raw)
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_taritarisuru(tokens)), [])

    def test_card_keeps_lexical_candidate_and_source(self):
        source = sentence('休日には本を読んだりする。', [
            tok('休日には本を', '本', ('名詞',)),
            tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            tok('だり', 'だり', ('助詞', '副助詞')),
            tok('する', 'する', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[EXPRESSION].frequency, 2)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, 'だりする')

    def test_existing_note_is_protected(self):
        existing = note(123, EXPRESSION, fields={'Meaning': 'user edited'})
        plans = []
        def invoke(action, params=None):
            if action == 'modelNames': return ['Custom']
            if action == 'findNotes': return [123]
            if action == 'notesInfo': return [existing]
            raise AssertionError(action)
        export_words_to_anki('Deck', {}, 'Custom', invoke=invoke, prune=True,
                             output_only=True, details_handler=plans.append)
        self.assertEqual(plans[0]['delete'], [])
        self.assertEqual(plans[0]['modify'], [])
        self.assertEqual(plans[0]['retained_existing_grammar'][0]['note_id'], 123)
