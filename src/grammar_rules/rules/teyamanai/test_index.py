import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.teyamanai.index import (
    TEYAMANAI_GRAMMAR_EXPRESSION,
    match_teyamanai,
)


class TeyamanaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        for text, expected in [
            ('成功を願ってやまない。', 'てやまない'),
            ('彼を信じてやまなかった。', 'てやまなかった'),
            ('彼を愛して止まない。', 'て止まない'),
            ('本を読んでやまない。', 'でやまない'),
            ('雨が止まない。', None), ('山が見えない。', None),
            ('願ってやむ。', None),
        ]:
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_teyamanai(tokens)],
                                 [] if expected is None else [expected])

    def test_card_keeps_lexical_candidate(self):
        source = sentence('私は彼の成功を願ってやまない。', [
            tok('私は彼の成功を', '成功', ('名詞',)),
            tok('願っ', '願う', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('やま', 'やむ', ('動詞', '一般')),
            tok('ない', 'ない', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {'願う': object()}
        stage.add_cards(words)
        self.assertIn('願う', words)
        self.assertEqual(words[TEYAMANAI_GRAMMAR_EXPRESSION].sentences[0].surface_form, 'てやまない')


if __name__ == '__main__':
    unittest.main()
