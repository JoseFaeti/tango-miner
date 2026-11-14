import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.uru_enai.index import RULE, match_uru_enai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class UruEnaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '起こり得る。': '得る', 'ありうる。': 'うる', 'ありえる。': 'える',
            '考え得ない。': '得ない', 'ありえなかった。': 'えなかった',
            '起こり得た。': '得た', '起こり得ます。': '得ます',
            '起こり得ませんでした。': '得ませんでした', '起こり得れば。': '得れば',
            '起こり得て。': '得て', 'あり得ぬ。': '得ぬ',
            '利益を得る。': None, '行かざるを得ない。': None,
            '起こり 得る。': None, '得ない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_uru_enai(tokens)],
                                 [] if expected is None else [expected])

    def test_incomplete_and_wrong_boundary(self):
        verb = tok('起こり', '起こる', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        for tail in ([tok('得', '得る', ('動詞',))],
                     [tok('得れ', '得る', ('動詞',))],
                     [tok('得', '得る', ('名詞',)), tok('ない', 'ない', ('助動詞',))]):
            raw = [verb] + tail
            text = ''.join(t['surface'] for t in raw)
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_uru_enai(tokens)), [])

    def test_card_retains_lexical_candidate_and_source(self):
        source = sentence('このような事故はいつでも起こり得るのです。', [
            tok('このような事故はいつでも', '事故', ('名詞',)),
            tok('起こり', '起こる', ('動詞', '一般', '*', '*', '五段', '連用形-一般')),
            tok('得る', '得る', ('動詞',)), tok('のです。', 'の', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'起こる': object()}
        stage.add_cards(words)
        self.assertIn('起こる', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '得る')


if __name__ == '__main__':
    unittest.main()
