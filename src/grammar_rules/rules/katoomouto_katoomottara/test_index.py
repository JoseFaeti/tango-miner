import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.katoomouto_katoomottara.index import (
    KATOOMOUTO_KATOOMOTTARA_EXPRESSION,
    match_katoomouto_katoomottara,
)
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KatoomoutoTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は帰ったかと思うと、また出かけた。': 'かと思うと',
            '雨が降ったかと思ったら晴れた。': 'かと思ったら',
            '行ったかとおもうと帰った。': 'かとおもうと',
            '飛んだかとおもったら落ちた。': 'かとおもったら',
            '行くかと思ったら違った。': None,
            '何かと思ったら猫だった。': None,
            '帰ったかと思った。': None,
            '帰ったか、と思ったら違った。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_katoomouto_katoomottara(tokens)],
                                 [] if expected is None else [expected])

    def test_card_reservation_and_attached_word(self):
        source = sentence('帰ったかと思うと出かけた。', [
            tok('帰っ', '帰る', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('か', 'か', ('助詞',)), tok('と', 'と', ('助詞', '格助詞')),
            tok('思う', '思う', ('動詞',)), tok('と', 'と', ('助詞', '接続助詞')),
            tok('出かけた。', '出かける', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['か', 'と', '思う', 'と'])
        words = {'帰る': object()}
        stage.add_cards(words)
        self.assertIn('帰る', words)
        self.assertEqual(words[KATOOMOUTO_KATOOMOTTARA_EXPRESSION].frequency, 2)
        self.assertEqual(words[KATOOMOUTO_KATOOMOTTARA_EXPRESSION].sentences[0].surface_form, 'かと思うと')
