import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.GrammarConstants import NJIYANAI_NODEHANAI_EXPRESSION as EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.njiyanai_nodehanai.index import match_njiyanai_nodehanai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NegativeExplanatoryTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨が降るんじゃないか。': 'んじゃない',
            'ここで遊ぶんじゃない。': 'んじゃない',
            '彼が来るんじゃなかった。': 'んじゃなかった',
            '今日は行くんではありません。': 'んではありません',
            '彼が来るのではないか。': 'のではない',
            '今日は行くんだ。': None,
            '学生じゃない。': None,
            '本を読んじゃった。': None,
            '行くのである。': None,
            '彼が来るんではある。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_njiyanai_nodehanai(tokens)],
                                 [] if expected is None else [expected])

    def test_card_preserves_example_and_lexical_candidate(self):
        source = sentence('ここで遊ぶんじゃない。', [
            tok('ここで', 'ここ', ('代名詞',)), tok('遊ぶ', '遊ぶ', ('動詞',)),
            tok('ん', 'ん', ('助詞', '準体助詞')), tok('じゃ', 'だ', ('助動詞',)),
            tok('ない', 'ない', ('形容詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'遊ぶ': object()}
        stage.add_cards(words)
        self.assertIn('遊ぶ', words)
        self.assertEqual(words[EXPRESSION].frequency, 2)
        self.assertEqual(words[EXPRESSION].sentences[0].surface_form, 'んじゃない')
        self.assertEqual(len(words[EXPRESSION].sentences), 1)
