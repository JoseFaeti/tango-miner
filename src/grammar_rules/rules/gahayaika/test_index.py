import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.gahayaika.index import RULE, match_gahayaika
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class GahayaikaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'ベルが鳴るが早いか、彼は飛び出した。': 'が早いか',
            '彼は帰ったが早いか、眠った。': 'が早いか',
            '食べるがはやいか走り出した。': 'がはやいか',
            '足が速いかどうか知らない。': None,
            '出発が早いか遅いか分からない。': None,
            '彼は静かだったが早いかどうかは知らない。': None,
            '鳴るが、早いかどうか知らない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_gahayaika(aligned)],
                                 [] if expected is None else [expected])

    def test_card_retains_attached_verb(self):
        source = sentence('鳴るが早いか飛び出した。', [
            tok('鳴る', '鳴る', ('動詞', '一般', '*', '*', '五段-ラ行', '終止形-一般')),
            tok('が', 'が', ('助詞', '格助詞')),
            tok('早い', '早い', ('形容詞', '一般')),
            tok('か', 'か', ('助詞', '副助詞')),
            tok('飛び出した。', '飛び出す', ('動詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'鳴る': object()}
        stage.add_cards(words)
        self.assertIn('鳴る', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'が早いか')


if __name__ == '__main__':
    unittest.main()
