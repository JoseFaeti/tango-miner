import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.shishi.index import RULES, match_shishi
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ShishiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '安いし、おいしいし、ここにしよう。': 2,
            '雨が降るし、行かない。': 1,
            '静かだし、便利です。': 1,
            '学生ですし、忙しい。': 1,
            '行かなかったし、知らない。': 1,
            '仕事をしている。': 0,
            '寿司を食べる。': 0,
        }
        for text, count in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_shishi(tokens)], ['し'] * count)

    def test_boundaries_and_card(self):
        predicate = tok('安い', '安い', ('形容詞', '一般', '*', '*', '*', '終止形-一般'))
        particle = tok('し', 'し', ('助詞', '接続助詞'))
        for text, raw in [('安い し', [predicate, particle]),
                          ('安いし', [predicate, tok('し', 'する', ('動詞',))])]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_shishi(tokens)), [])
        source = sentence('この店は安いし便利なのでよく行きます。', [
            tok('この店は', '店', ('名詞',)), predicate, particle,
            tok('便利なのでよく行きます。', '便利', ('名詞',))])
        stage = GrammarStep(rules=RULES)
        stage.process(Artifact([source, source]))
        words = {'安い': object()}
        stage.add_cards(words)
        self.assertIn('安い', words)
        self.assertEqual(words[RULES[0].expression].frequency, 2)
        self.assertEqual(words[RULES[0].expression].sentences[0].surface_form, 'し')
