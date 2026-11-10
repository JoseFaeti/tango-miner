import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import MADENI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.madeni.index import match_madeni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MadeniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '明日までに終わる。': 'までに', '帰るまでに終える。': 'までに',
            '死ぬまでに行く。': 'までに', 'これまでに見た。': 'までに',
            'それまでに終える。': 'までに', 'その時までに帰る。': 'までに',
            'ここまでに終える。': 'までに',
            'これほどまでに禍々しい。': None, 'それほどまでに思う。': None,
            'これ程までに恐ろしい。': None, 'ここまでに美しい。': None,
            'あそこまでに強い。': None,
            '明日まで待つ。': None, '今までに見た。': None,
            'までに': None, '真似をする。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_madeni(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card_evidence(self):
        source = sentence('私は明日までに家に帰ります。', [
            tok('私は', '私', ('名詞',)), tok('明日', '明日', ('名詞',)),
            tok('まで', 'まで', ('助詞', '副助詞')),
            tok('に', 'に', ('助詞', '格助詞')), tok('家に帰ります。', '帰る', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['まで', 'に'])
        words = {'明日': object()}
        stage.add_cards(words)
        self.assertIn('明日', words)
        self.assertEqual(words[MADENI_EXPRESSION].frequency, 2)
        self.assertEqual(words[MADENI_EXPRESSION].sentences[0].surface_form, 'までに')

    def test_rejects_nonadjacent_and_wrong_pos(self):
        for text, raw in [
            ('明日まで に', [tok('明日', '明日', ('名詞',)), tok('まで', 'まで', ('助詞', '副助詞')), tok('に', 'に', ('助詞', '格助詞'))]),
            ('明日までに', [tok('明日', '明日', ('名詞',)), tok('まで', 'まで', ('名詞',)), tok('に', 'に', ('助詞', '格助詞'))]),
        ]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_madeni(tokens)), [])

    def test_degree_expression_is_not_reserved(self):
        source = sentence('これはここまでに美しいものです。', [
            tok('これは', 'これ', ('代名詞',)), tok('ここ', 'ここ', ('代名詞',)),
            tok('まで', 'まで', ('助詞', '副助詞')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('美しい', '美しい', ('形容詞',)), tok('ものです。', 'もの', ('名詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertFalse(any(t.get('grammar_reserved') for t in data.data[0].tokens))
        self.assertEqual(stage.occurrences['madeni'].count, 0)
