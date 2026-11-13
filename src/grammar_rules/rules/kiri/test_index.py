import unittest

from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kiri.index import KIRI_EXPRESSION, match_kiri
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.Artifact import Artifact
from src.Grammar import GrammarStep


class KiriTests(unittest.TestCase):
    def test_sudachi_forms_and_lexical_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'二人きりで話す。': 'きり', '一度きりだ。': 'きり',
                 '出たきり帰らない。': 'きり', 'それきり会わない。': 'きり',
                 'これっきりだ。': 'っきり', '寝たっきりだ。': 'っきり',
                 '紙を切り取る。': None, 'きりがない。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kiri(tokens)],
                                 [] if expected is None else [expected])

    def test_card_retains_attached_word(self):
        source = sentence('私たちは二人きりで話しました。', [
            tok('私たちは', '私', ('名詞',)),
            tok('二人', '二人', ('名詞',)), tok('きり', 'きり', ('助詞', '副助詞')),
            tok('で話しました。', 'で', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {'二人': object()}
        stage.add_cards(words)
        self.assertIn('二人', words)
        self.assertEqual(words[KIRI_EXPRESSION].sentences[0].surface_form, 'きり')


if __name__ == '__main__':
    unittest.main()
