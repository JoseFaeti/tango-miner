import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import GARU_GRAMMAR_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.garu.index import match_garu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class GaruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'痛がる。': 'がる', '怖がった。': 'がった', '痛がらない。': 'がらない',
                 '痛がらなかった。': 'がらなかった', '痛がります。': 'がります',
                 '痛がりませんでした。': 'がりませんでした', '痛がっている。': 'がって',
                 '嫌がる。': 'がる', '不思議がる。': 'がる', '欲しがる。': 'がる',
                 '遊びたがる。': None, '疑る。': None, '上がる。': None, '痛がり。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_garu(aligned)],
                                 [] if expected is None else [expected])

    def test_incomplete_and_unrelated_stems(self):
        stem = tok('痛', '痛い', ('形容詞', '一般', '*', '*', '形容詞', '語幹-一般'))
        for left, suffix in [(stem, tok('がっ', 'がる', ('接尾辞', '動詞的'))),
                             (tok('本', '本', ('名詞', '普通名詞', '一般')),
                              tok('がる', 'がる', ('接尾辞', '動詞的'))),
                             (stem, tok('がる', 'がる', ('動詞', '一般')))]:
            text = left['surface'] + suffix['surface']
            tokens, error = aligned_tokens(text, [left, suffix])
            self.assertIsNone(error)
            self.assertEqual(list(match_garu(tokens)), [])

    def test_card_keeps_attached_word(self):
        source = sentence('彼はとても痛がるそうです。', [tok('彼はとても', '彼', ('名詞',)),
            tok('痛', '痛い', ('形容詞', '一般', '*', '*', '形容詞', '語幹-一般')),
            tok('がる', 'がる', ('接尾辞', '動詞的')), tok('そうです。', 'そう', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'痛い': object()}
        stage.add_cards(words)
        self.assertIn('痛い', words)
        self.assertEqual(words[GARU_GRAMMAR_EXPRESSION].frequency, 2)
        self.assertEqual(words[GARU_GRAMMAR_EXPRESSION].sentences[0].surface_form, 'がる')
