import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import KUSURU_NISURU_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kusuru_nisuru.index import match_kusuru_nisuru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MakeChooseTests(unittest.TestCase):
    def test_source_tokenizations(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '部屋を明るくする。': '明るくする', '部屋を明るくした。': '明るくした',
            '音を大きくしない。': '大きくしない', '音を大きくしてください。': '大きくして',
            '部屋をきれいにします。': 'にします', 'お茶にする。': 'にする',
            '静かにしなかった。': 'にしなかった', '安くすればいい。': '安くすれば',
            '本を読むようにする。': None, '行くことにする。': None,
            '高く売る。': None, '駅に行く。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kusuru_nisuru(aligned)],
                                 [] if expected is None else [expected])

    def test_incomplete_and_separated(self):
        adjective = tok('高く', '高い', ('形容詞', '*', '*', '*', '*', '連用形-一般'))
        for text, raw in [
            ('高くし', [adjective, tok('し', 'する', ('動詞',))]),
            ('高く する', [adjective, tok('する', 'する', ('動詞',))]),
        ]:
            aligned, error = aligned_tokens(text, raw)
            if error is None:
                self.assertEqual(list(match_kusuru_nisuru(aligned)), [])

    def test_card_preserves_lexical_evidence(self):
        source = sentence('私は部屋を明るくしたと思います。', [
            tok('私は部屋を', '部屋', ('名詞',)),
            tok('明るく', '明るい', ('形容詞', '*', '*', '*', '*', '連用形-一般')),
            tok('し', 'する', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        artifact = Artifact([source, source])
        stage.process(artifact)
        words = {'明るい': object()}
        stage.add_cards(words)
        self.assertIn('明るい', words)
        self.assertEqual(words[KUSURU_NISURU_EXPRESSION].frequency, 2)
        self.assertEqual(words[KUSURU_NISURU_EXPRESSION].sentences[0].surface_form, '明るくした')
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))


if __name__ == '__main__':
    unittest.main()
