import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import TAGARU_GRAMMAR_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tagaru import match_tagaru
from src.grammar_rules.rules.tests.test_grammar_te_iru import sentence, tok


class TagaruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'遊びたがる。': 'たがる', '読みたがった。': 'たがった',
                 '行きたがらない。': 'たがらない', '行きたがらなかった。': 'たがらなかった',
                 '食べたがります。': 'たがります', '食べたがりませんでした。': 'たがりませんでした',
                 '遊びたがっている。': 'たがって', '本を読みたい。': None,
                 '痛がる。': None, '疑る。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tagaru(aligned)],
                                 [] if expected is None else [expected])

    def test_incomplete_and_wrong_pos_are_not_matched(self):
        verb = tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        for part in [tok('たがっ', 'たがる', ('助動詞',)),
                     tok('たがる', 'たがる', ('名詞',))]:
            text = verb['surface'] + part['surface']
            tokens, error = aligned_tokens(text, [verb, part])
            self.assertIsNone(error)
            self.assertEqual(list(match_tagaru(tokens)), [])

    def test_card_keeps_attached_word(self):
        source = sentence('彼はいつも本を読みたがるそうです。', [tok('彼はいつも本を', '本', ('名詞',)),
            tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('たがる', 'たがる', ('助動詞',)), tok('そうです。', 'そう', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[TAGARU_GRAMMAR_EXPRESSION].frequency, 2)
        self.assertEqual(words[TAGARU_GRAMMAR_EXPRESSION].sentences[0].surface_form, 'たがる')
