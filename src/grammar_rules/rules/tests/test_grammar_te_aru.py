import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.te_aru import RULE, match_te_aru
from src.grammar_rules.rules.tests.test_grammar_te_iru import sentence, tok


class TeAruTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '窓が開けてある。': 'てある', '名前が書いてあった。': 'てあった',
            '本が置いてあります。': 'てあります', '準備してありませんでした。': 'てありませんでした',
            '書いてあればいい。': 'てあれば', '書いてあってよかった。': 'てあって',
            '置いて有る。': 'て有る', '本がある。': None,
            '書いてない。': None, '本を読んでいる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_aru(aligned)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_reservation(self):
        source = sentence('窓が開けてあったよ。', [
            tok('窓が', '窓', ('名詞',)),
            tok('開け', '開ける', ('動詞', '一般', '*', '*', '下一段', '連用形-一般')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('あっ', 'ある', ('動詞', '非自立可能')),
            tok('た', 'た', ('助動詞',)), tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        reserved = ExcludeGrammarTokensStep(stage).process(data).data[0].tokens
        self.assertEqual([t['surface'] for t in reserved if t.get('grammar_reserved')], ['て', 'あっ', 'た'])
        words = {'開ける': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'てあった')
        self.assertIn('開ける', words)
        aligned, error = aligned_tokens(source.text, source.tokens)
        self.assertIsNone(error)
        self.assertEqual(list(match_te_aru(aligned[:-2])), [])
        aligned[3]['span'][0] += 1
        self.assertEqual(list(match_te_aru(aligned)), [])


if __name__ == '__main__':
    unittest.main()
