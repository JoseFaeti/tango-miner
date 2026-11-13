import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.shidai.index import RULE, match_shidai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ShidaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '準備ができ次第連絡します。': '次第',
            '到着次第お知らせします。': '次第',
            '準備ができしだい出発する。': 'しだい',
            '努力次第だ。': None, '結果次第で決める。': None,
            '次第に暗くなる。': None, '事の次第を話す。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_shidai(aligned)],
                                 [] if expected is None else [expected])

    def test_card_retains_attached_verb(self):
        source = sentence('準備ができ次第連絡する予定です。', [
            tok('準備が', '準備', ('名詞',)),
            tok('でき', 'できる', ('動詞', '非自立可能', '*', '*', '上一段', '連用形-一般')),
            tok('次第', '次第', ('名詞', '普通名詞', '副詞可能')),
            tok('連絡', '連絡', ('名詞',)), tok('する', 'する', ('動詞',)),
            tok('予定です。', '予定', ('名詞',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'できる': object()}
        stage.add_cards(words)
        self.assertIn('できる', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, '次第')


if __name__ == '__main__':
    unittest.main()
