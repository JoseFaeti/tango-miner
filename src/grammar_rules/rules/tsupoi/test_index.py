import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import TSUPOI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tsupoi.index import match_tsupoi
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TsupoiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '子供っぽい人だ。': 'っぽい', '忘れっぽかった。': 'っぽかった',
            '飽きっぽくない。': 'っぽくない', '水っぽくて困る。': 'っぽくて',
            '白っぽければいい。': 'っぽければ', '子供っぽいです。': 'っぽいです',
            '尻尾を振る。': None, '一方通行だ。': None, 'っぽいね。': None,
            '安っぽい服だ。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tsupoi(aligned)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card_reservation(self):
        noun = tok('子供', '子供', ('名詞',))
        suffix = tok('っぽい', 'っぽい', ('接尾辞', '形容詞的'))
        source = sentence('あの人は子供っぽいと思います。', [
            tok('あの人は', '人', ('名詞',)), noun, suffix,
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'子供': object()}
        stage.add_cards(words)
        self.assertIn('子供', words)
        self.assertEqual(words[TSUPOI_EXPRESSION].frequency, 2)
        self.assertEqual(words[TSUPOI_EXPRESSION].sentences[0].surface_form, 'っぽい')
        result = ExcludeGrammarTokensStep(stage).process(Artifact([source, source]))
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved', False))
        self.assertTrue(result.data[0].tokens[2].get('grammar_reserved', False))
        for parts in ([noun, tok('っぽかっ', 'っぽい', ('接尾辞', '形容詞的'))],
                      [noun, tok('っぽい', 'っぽい', ('名詞',))]):
            text = ''.join(t['surface'] for t in parts)
            aligned, error = aligned_tokens(text, parts)
            self.assertIsNone(error)
            self.assertEqual(list(match_tsupoi(aligned)), [])
