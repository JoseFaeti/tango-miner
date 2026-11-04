import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import NAGARA_GRAMMAR_EXPRESSION as EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nagara.index import match_nagara
from src.steps.AddWordsToAnkiStep.index import export_words_to_anki
from src.steps.AddWordsToAnkiStep.test_index import note
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NagaraTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '音楽を聴きながら歩く。': True,
            '食べながら話す。': True,
            '知りながら黙っていた。': True,
            '残念ながら行けない。': False,
            '生まれながらの才能。': False,
            'ながらく待つ。': False,
            '読みながら。': False,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nagara(aligned)],
                                 ['ながら'] if expected else [])

    def test_card_retains_attached_verb_and_protects_identity(self):
        source = sentence('私は本を読みながら歩きました。', [
            tok('私は本を', '本', ('名詞',)),
            tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('ながら', 'ながら', ('助詞', '接続助詞')),
            tok('歩きました。', '歩く', ('動詞',))])
        stage = GrammarStep()
        artifact = Artifact([source, source])
        stage.process(artifact)
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[EXPRESSION].frequency, 2)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, 'ながら')
        self.assertEqual(len(words[EXPRESSION].sentences), 1)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertTrue(artifact.data[0].tokens[2].get('grammar_reserved'))
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))
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
        self.assertEqual(plans[0]['retained_existing_grammar'][0]['note_id'], 123)
