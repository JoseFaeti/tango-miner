import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.no_n.index import NO_N_EXPRESSION, match_no_n
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NoNTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'どこへ行くの？': 'の', 'もう食べたの。': 'の',
                 '高いの？': 'の', '静かなの？': 'の', '学生なの？': 'の',
                 '行くのか？': None, '行くんだ。': None,
                 '私の本。': None, '読むのが好き。': None, '読んだ。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_no_n(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_boundaries(self):
        verb = tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '終止形-一般'))
        marker = tok('ん', 'ん', ('助詞', '準体助詞'))
        source = sentence('明日は学校に行くん？', [tok('明日は学校に', '学校', ('名詞',)), verb, marker,
                                            tok('？', '？', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source, source]))
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[NO_N_EXPRESSION].frequency, 2)
        self.assertEqual(words[NO_N_EXPRESSION].sentences[0].surface_form, 'ん')
        for text, raw in [('行く ん', [verb, marker]),
                          ('行くんだ', [verb, marker, tok('だ', 'だ', ('助動詞',))]),
                          ('行くん', [verb, tok('ん', 'ん', ('助動詞',))])]:
            aligned, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_no_n(aligned)), [])
