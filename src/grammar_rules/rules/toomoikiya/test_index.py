import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.toomoikiya.index import TOOMOIKIYA_EXPRESSION, match_toomoikiya
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToomoikiyaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降ると思いきや晴れた。': 'と思いきや',
            '終わったと思いきや、まだ続いた。': 'と思いきや',
            '簡単かと思いきや難しい。': 'と思いきや',
            '雨かとおもいきや晴れた。': 'とおもいきや',
            '終わったかと思いきや、まだ続いた。': 'と思いきや',
            '雨かと、思いきや晴れた。': None,
            '雨かと思い切った。': None,
            '雨だとおもいきや晴れた。': 'とおもいきや',
            'そうと思い切った。': None,
            '雨だと思った。': None,
            '雨だと、思いきや晴れた。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toomoikiya(tokens)],
                                 [] if expected is None else [expected])

    def test_card_reservation_and_attached_word(self):
        source = sentence('雨かと思いきや晴れた。', [
            tok('雨', '雨', ('名詞',)), tok('か', 'か', ('助詞',)),
            tok('と', 'と', ('助詞', '格助詞')), tok('思い', '思う', ('動詞',)),
            tok('き', 'き', ('助動詞',)), tok('や', 'や', ('助詞',)),
            tok('晴れた。', '晴れる', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['と', '思い', 'き', 'や'])
        words = {'雨': object()}
        stage.add_cards(words)
        self.assertIn('雨', words)
        self.assertEqual(words[TOOMOIKIYA_EXPRESSION].frequency, 2)
        self.assertEqual(words[TOOMOIKIYA_EXPRESSION].sentences[0].surface_form, 'と思いきや')
        self.assertNotIn('〜かと思いきや', words)
