import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import IRAI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.irai.index import match_irai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class IraiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '卒業以来会っていない。': '以来', '去年以来初めて会う。': '以来',
            'それ以来会わない。': '以来', '明治以来の歴史。': '以来',
            '依頼を受ける。': None, '以来ずっと続く。': None,
            '日本に来て以来住んでいる。': None, '卒業いらい会わない。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_irai(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card_evidence(self):
        source = sentence('私は卒業以来会っていない。', [
            tok('私は', '私', ('名詞',)), tok('卒業', '卒業', ('名詞',)),
            tok('以来', '以来', ('名詞',)), tok('会っていない。', '会う', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['以来'])
        words = {'卒業': object()}
        stage.add_cards(words)
        self.assertIn('卒業', words)
        self.assertEqual(words[IRAI_EXPRESSION].frequency, 2)
        self.assertEqual(words[IRAI_EXPRESSION].sentences[0].surface_form, '以来')
        for text, raw in [
            ('卒業 以来', [tok('卒業', '卒業', ('名詞',)), tok('以来', '以来', ('名詞',))]),
            ('卒業以来', [tok('卒業', '卒業', ('名詞',)), tok('以来', '以来', ('動詞',))]),
        ]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual(list(match_irai(tokens)), [])


if __name__ == '__main__':
    unittest.main()
