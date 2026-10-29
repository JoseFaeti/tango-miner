import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.rules.te_shimau import match_te_shimau
from src.GrammarConstants import TE_SHIMAU_GRAMMAR_EXPRESSION as EXPRESSION
from src.GrammarShadow import aligned_tokens
from src.steps.AddWordsToAnkiStep import export_words_to_anki
from tests.test_add_words_to_anki_step import note
from tests.test_grammar_te_iru import sentence, tok


class TeShimauTests(unittest.TestCase):
    def test_tokenizer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読んでしまう。': 'でしまう', '本を読んでしまった。': 'でしまった',
            '宿題をしてしまいました。': 'てしまいました',
            '本を読んでしまわない。': 'でしまわない',
            '本を読んじゃう。': 'じゃう', '本を読んじゃった。': 'じゃった',
            '本を読んじゃいます。': 'じゃいます', '本を読んじゃわない。': 'じゃわない',
            '本を見ちゃう。': 'ちゃう', '本を見ちゃった。': 'ちゃった',
            'お茶を飲む。': None, 'それじゃない。': None,
            'しまう場所がない。': None, '話しちゃだめだよ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_shimau(aligned)],
                                 [] if expected is None else [expected])

    def test_card_keeps_lexical_candidates_and_source_span(self):
        source = sentence('私は本を読んでしまったよ。', [
            tok('私は本を', '本', ('名詞',)),
            tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            tok('で', 'で', ('助詞', '接続助詞')),
            tok('しまっ', 'しまう', ('動詞', '非自立可能')),
            tok('た', 'た', ('助動詞',)), tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[EXPRESSION].frequency, 2)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, 'でしまった')
        self.assertEqual(len(words[EXPRESSION].sentences), 1)

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


if __name__ == '__main__':
    unittest.main()
