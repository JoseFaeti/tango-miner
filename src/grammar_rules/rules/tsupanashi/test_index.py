import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.tsupanashi.index import TSUPANASHI_EXPRESSION, match_tsupanashi


class TsupanashiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '窓を開けっぱなしだ。': 'っぱなし', '立ちっぱなしで疲れた。': 'っぱなし',
            '本を出しっぱなしの部屋。': 'っぱなし', '電気をつけっぱなしにする。': 'っぱなし',
            '言いっぱなしだった。': 'っぱなし', '梨を食べる。': None,
            'っぱなしだけ聞こえた。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tsupanashi(aligned)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_reservation(self):
        verb = tok('開け', '開ける', ('動詞', '一般', '*', '*', '下一段', '連用形-一般'))
        suffix = tok('っぱなし', 'っぱなし', ('接尾辞', '形状詞的'))
        source = sentence('窓を開けっぱなしにした。', [
            tok('窓を', '窓', ('名詞',)), verb, suffix, tok('にした。', 'に', ('助詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'開ける': object()}
        stage.add_cards(words)
        self.assertIn('開ける', words)
        self.assertEqual(words[TSUPANASHI_EXPRESSION].frequency, 2)
        self.assertEqual(words[TSUPANASHI_EXPRESSION].sentences[0].surface_form, 'っぱなし')
        result = ExcludeGrammarTokensStep(stage).process(Artifact([source]))
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved', False))
        self.assertTrue(result.data[0].tokens[2].get('grammar_reserved', False))
        for parts in ([tok('開ける', '開ける', ('動詞', '一般', '*', '*', '下一段', '終止形')), suffix],
                      [tok('開け', '開け', ('名詞',)), suffix],
                      [verb, tok('っぱなし', 'っぱなし', ('名詞',))],
                      [tok('開けっぱなし', '開ける', ('動詞',))]):
            text = ''.join(t['surface'] for t in parts)
            aligned, error = aligned_tokens(text, parts)
            self.assertIsNone(error)
            self.assertEqual(list(match_tsupanashi(aligned)), [])
