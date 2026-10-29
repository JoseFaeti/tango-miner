import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.rules.nda import match_nda
from src.GrammarShadow import aligned_tokens
from src.GrammarConstants import NDA_GRAMMAR_EXPRESSION as EXPRESSION
from tests.test_grammar_te_iru import sentence, tok
from tests.test_add_words_to_anki_step import note
from src.steps.AddWordsToAnkiStep import export_words_to_anki


class NdaCardTests(unittest.TestCase):
    def test_observed_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '今日は行くんだよ。': 'んだ',
            '今日は行くんです。': 'んです',
            '昨日行ったんだった。': 'んだった',
            '今日は行くんでした。': 'んでした',
            '今日は行くんでしょう。': 'んでしょう',
            '今日は行くんだろう。': 'んだろう',
            '今日は行くんじゃない。': 'んじゃない',
            '彼が来るんじゃなかった。': 'んじゃなかった',
            'ともにほろびたんじゃなかろうか。': 'んじゃなかろう',
            '今日は行くんではありません。': 'んではありません',
            '今日は行くんじゃありませんでした。': 'んじゃありませんでした',
            '行くのである。': 'のである',
            '彼は行くのだ。': 'のだ',
            '今日は行くのです。': 'のです',
            '彼は元気なのです。': 'のです',
            'これは僕のです。': None,
            'その本を読んだ。': None,
            '水を飲んでいる。': None,
            '今日は行くの？': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                found = [text[start:end] for start, end in match_nda(aligned)]
                self.assertEqual(found, [] if expected is None else [expected])

    def test_supported_occurrence_preserves_lexical_candidate_and_source(self):
        source = sentence('今日は学校に行くんだよ。', [
            tok('今日は学校に', '学校', ('名詞',)), tok('行く', '行く', ('動詞',)),
            tok('ん', 'ん', ('助詞', '準体助詞')), tok('だ', 'だ', ('助動詞',)),
            tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'んだ': object()}
        stage.add_cards(words)
        card = words[EXPRESSION]
        self.assertEqual(card.frequency, 2)
        self.assertEqual(len(card.sentences), 1)
        self.assertEqual(card.sentences[0].surface_form, 'んだ')
        self.assertIn('んだ', words)
        self.assertEqual(card.tags, {'source'})

    def test_ambiguous_and_unaligned_analyses_do_not_create_card(self):
        cases = [
            sentence('昨日あの人が死んだんです。', [tok('昨日あの人が', '人', ('名詞',)),
                     tok('死ん', '死ぬ', ('動詞',)), tok('だ', 'た', ('助動詞',)),
                     tok('んです。', 'ん', ('助詞',))]),
            sentence('そうなんだと思いますよ。', [tok('そうな', 'そう', ('名詞',)),
                     tok('んだ', 'んだ', ('名詞',)), tok('と思いますよ。', 'と', ('助詞',))]),
            sentence('今日は行くんだと思います。', [tok('今日は行く', '行く', ('動詞',)),
                     tok('ん', 'ん', ('助詞', '準体助詞')), tok('だ', 'だ', ('助動詞',))]),
        ]
        stage = GrammarStep()
        stage.process(Artifact(cases))
        words = {}
        stage.add_cards(words)
        self.assertNotIn(EXPRESSION, words)

    def test_existing_card_is_protected_from_update_and_pruning(self):
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
