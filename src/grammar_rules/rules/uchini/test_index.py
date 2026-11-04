import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import UCHINI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.uchini.index import match_uchini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class UchiniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '食べるうちに慣れる。': 'うちに', '食べないうちに帰る。': 'うちに',
            '若いうちに学ぶ。': 'うちに', '元気なうちに行く。': 'うちに',
            '休みのうちに読む。': 'うちに', '気づかない内に終わる。': '内に',
            'うちに帰る。': None, 'そのうちに行く。': None,
            '内に秘める。': None, 'うちには帰らない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_uchini(aligned)],
                                 [] if expected is None else [expected])

    def test_card_preserves_attached_word_and_evidence(self):
        source = sentence('私は休みのうちに本を読みます。', [
            tok('私は', '私', ('名詞',)),
            tok('休み', '休み', ('名詞',)), tok('の', 'の', ('助詞', '格助詞')),
            tok('うち', 'うち', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
            tok('本を読みます。', '読む', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['うち', 'に'])
        words = {'休み': object()}
        stage.add_cards(words)
        self.assertIn('休み', words)
        self.assertEqual(words[UCHINI_EXPRESSION].frequency, 2)
        self.assertEqual(words[UCHINI_EXPRESSION].sentences[0].surface_form, 'うちに')


if __name__ == '__main__':
    unittest.main()
