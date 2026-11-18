import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.yainaya.index import RULE, match_yainaya
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class YainayaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'ベルが鳴るや否や飛び出した。': 'や否や',
            '彼は見るやいなや逃げた。': 'やいなや',
            '行くや否や': 'や否や',
            '行ったや否や': None,
            '賛成や否やを問う。': None,
            '否やを述べる。': None,
            '鳴るや、否やを述べた。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_yainaya(aligned)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation_retain_verb(self):
        source = sentence('鳴るや否や飛び出した。', [
            tok('鳴る', '鳴る', ('動詞', '一般', '*', '*', '五段-ラ行', '終止形-一般')),
            tok('や', 'や', ('助詞', '副助詞')),
            tok('否', '否', ('名詞', '普通名詞')),
            tok('や', 'や', ('助詞', '副助詞')),
            tok('飛び出した。', '飛び出す', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = Artifact([source, source])
        stage.process(artifact)
        words = {'鳴る': object()}
        stage.add_cards(words)
        self.assertIn('鳴る', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'や否や')
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([bool(t.get('grammar_reserved')) for t in artifact.data[0].tokens],
                         [False, True, True, True, False])


if __name__ == '__main__':
    unittest.main()
