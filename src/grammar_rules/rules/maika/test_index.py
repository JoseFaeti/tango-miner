import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.maika.index import RULE, match_maika
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MaikaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '彼は来るまいか。': 'まいか', '彼は来まいかと思う。': 'まいか',
            '一緒に行くまいか。': 'まいか', '彼は食べまいか。': 'まいか',
            '彼は犯人ではあるまいか。': None, '犯人じゃあるまいか。': None,
            'もう行くまい。': None, 'うまい料理だ。': None,
            '行くまいかしら。': None, '行くまい か。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_maika(tokens)],
                                 [] if expected is None else [expected])

    def test_card_reservation_and_overlap(self):
        source = sentence('彼は明日そこへ行くまいかと思います。', [
            tok('彼は明日そこへ', '彼', ('名詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段-カ行', '終止形-一般')),
            tok('まい', 'まい', ('助動詞',)), tok('か', 'か', ('助詞', '終助詞')),
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['まい', 'か'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'まいか')
        self.assertIn('〜まい', words)


if __name__ == '__main__':
    unittest.main()
