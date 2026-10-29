import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.rules.tai import match_tai
from src.GrammarConstants import TAI_GRAMMAR_EXPRESSION as EXPRESSION
from src.GrammarShadow import aligned_tokens
from src.steps.AddWordsToAnkiStep import export_words_to_anki
from tests.test_add_words_to_anki_step import note
from tests.test_grammar_te_iru import sentence, tok


class TaiCardTests(unittest.TestCase):
    def test_observed_forms_and_unrelated_words(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読みたい。': 'たい', '本を読みたかった。': 'たかった',
            '本を読みたくない。': 'たくない', '本を読みたくなかった。': 'たくなかった',
            '死にたくなかったら街を出る。': 'たくなかったら',
            '看板汚したくなきゃ。': 'たくなきゃ',
            '本を読みたいです。': 'たいです', '本を読みたければ読んで。': 'たければ',
            'お茶を飲みたくて来た。': 'たくて', '鯛を食べる。': None,
            'それは痛い。': None, 'あれは猫みたいだ。': None,
            '遊びたがっている。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tai(aligned)],
                                 [] if expected is None else [expected])

    def test_inflection_boundaries_and_lexical_controls(self):
        verb = tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        def matches(parts):
            raw = [verb] + parts
            text = ''.join(t['surface'] for t in raw)
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            return [text[a:b] for a, b in match_tai(aligned)]
        desire = lambda surface: tok(surface, 'たい', ('助動詞',))
        past = tok('た', 'た', ('助動詞',))
        negative = tok('ない', 'ない', ('形容詞',))
        conditional = tok('ば', 'ば', ('助詞',))
        for parts, expected in [
            ([desire('たかっ'), past], ['たかった']),
            ([desire('たく'), negative], ['たくない']),
            ([desire('たけれ'), conditional], ['たければ']),
            ([desire('たかっ')], []),
            ([desire('たけれ')], []),
            ([desire('たく'), tok('なけれ', 'ない', ('形容詞',))], []),
            ([tok('たい', 'たい', ('名詞',))], []),
        ]:
            with self.subTest(parts=parts):
                self.assertEqual(matches(parts), expected)

    def test_shared_matcher_allows_caller_to_select_forms(self):
        from src.grammar_rules.helpers import adjective_inflection_end
        raw = [tok('高けれ', '高い', ('形容詞',)), tok('ば', 'ば', ('助詞',))]
        tokens, error = aligned_tokens('高ければ', raw)
        self.assertIsNone(error)
        kwargs = dict(lemma='高い', pos='形容詞', base='高い', past_stem='高かっ',
                      continuative_stem='高く', conditional_stem='高けれ')
        self.assertEqual(adjective_inflection_end(tokens, 0, forms=('conditional',), **kwargs), 1)
        self.assertIsNone(adjective_inflection_end(tokens, 0, forms=('base',), **kwargs))

    def test_card_uses_source_and_keeps_lexical_candidates(self):
        source = sentence('私は本を読みたいと思います。', [
            tok('私は本を', '本', ('名詞',)),
            tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('たい', 'たい', ('助動詞',)), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[EXPRESSION].frequency, 2)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, 'たい')
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
