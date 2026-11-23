import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kainaka.index import KAINAKA_EXPRESSION, match_kainaka
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class KainakaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行くか否か分からない。': 'か否か',
            '行かないか否か確認する。': 'か否か',
            '高いか否か聞く。': 'か否か',
            '静かか否か調べる。': 'か否か',
            '学生か否か知らない。': 'か否か',
            '雨だったか否か覚えていない。': 'か否か',
            '否か応かを問う。': None,
            '行くか、否かを問う。': None,
            '行くか田舎へ帰るか。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kainaka(tokens)],
                                 [] if expected is None else [expected], raw)

    def test_card_reservation_and_precedence(self):
        source = sentence('行くか否か分からない。', [
            tok('行く', '行く', ('動詞',)), tok('か', 'か', ('助詞',)),
            tok('否', '否', ('名詞',)), tok('か', 'か', ('助詞',)),
            tok('分からない。', '分かる', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['か', '否', 'か'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn(KAINAKA_EXPRESSION, GRAMMAR_EXPRESSIONS)
        self.assertIn('行く', words)
        self.assertNotIn('〜か〜か', words)
        self.assertEqual(words[KAINAKA_EXPRESSION].frequency, 2)
        self.assertEqual(words[KAINAKA_EXPRESSION].sentences[0].surface_form, 'か否か')

    def test_boundaries_and_lexical_controls(self):
        for text, raw in [
            ('行くか 否か', [tok('行く', '行く', ('動詞',)), tok('か', 'か', ('助詞',)), tok('否', '否', ('名詞',)), tok('か', 'か', ('助詞',))]),
            ('行くか否か', [tok('行く', '行く', ('動詞',)), tok('か', 'か', ('助詞',)), tok('否', '否', ('感動詞',)), tok('か', 'か', ('助詞',))]),
        ]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_kainaka(tokens)), [])
