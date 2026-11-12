import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import TARINANKASHITE_EXPRESSION as EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tarinankashite.index import match_tarinankashite
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TarinankashiteTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '映画を見たりなんかして。': 'たりなんかして',
            '本を読んだりなんかして。': 'だりなんかして',
            '高かったりなんかして。': 'たりなんかして',
            '映画を見たりする。': None,
            '映画を見たりなんかした。': None,
            '映画を見たり、なんかして。': None,
            '二人なんかして。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tarinankashite(tokens)],
                                 [] if expected is None else [expected])

    def test_card_preserves_attached_word(self):
        source = sentence('本を読んだりなんかして。', [
            tok('本を', '本', ('名詞',)),
            tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            tok('だり', 'だり', ('助詞', '副助詞')),
            tok('なんか', 'なんか', ('助詞', '副助詞')),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞', '接続助詞')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, 'だりなんかして')
