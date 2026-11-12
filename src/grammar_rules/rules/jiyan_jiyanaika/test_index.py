import unittest

from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.jiyan_jiyanaika.index import match_jiyan_jiyanaika
from src.grammar_rules.rules.te_iru.test_index import tok, sentence
from src.GrammarConstants import JIYAN_JIYANAIKA_EXPRESSION as EXPRESSION
from src.Artifact import Artifact
from src.Grammar import GrammarStep


class JiyanTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'いいじゃん。': 'じゃん', '学生じゃん。': 'じゃん',
                 '行くじゃないか。': 'じゃないか', '学生じゃないか。': 'じゃないか',
                 '行かないじゃないか。': 'じゃないか', 'じゃんけんをする。': None,
                 '学生じゃない。': None, '行かないか。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_jiyan_jiyanaika(tokens)],
                                 [] if expected is None else [expected])

    def test_card_keeps_lexical_candidate_and_examples(self):
        source = sentence('それはとてもいいじゃん。', [
            tok('それはとても', 'それ', ('代名詞',)),
            tok('いい', 'いい', ('形容詞', '非自立可能', '*', '*', '形容詞', '終止形-一般')),
            tok('じゃん', 'じゃん', ('助詞', '終助詞')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'いい': object()}
        stage.add_cards(words)
        self.assertIn('いい', words)
        self.assertEqual(words[EXPRESSION].frequency, 2)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, 'じゃん')
        self.assertEqual(len(words[EXPRESSION].sentences), 1)
