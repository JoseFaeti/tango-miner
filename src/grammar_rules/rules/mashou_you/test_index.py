import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import MASHOU_YOU_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.mashou_you.index import match_mashou_you
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MashouYouTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読もう。': '読もう', '食べよう。': '食べよう',
            '来よう。': '来よう', '勉強しよう。': 'しよう',
            '一緒に行きましょう。': 'ましょう', '行きましょ。': 'ましょ',
            '鳥のようだ。': None, '本を読む。': None, '読みました。': None,
            '食べようと思う。': '食べよう',
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_mashou_you(tokens)],
                                 [] if expected is None else [expected])

    def test_boundaries_and_card_integration(self):
        verb = tok('読み', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-一般'))
        polite = tok('ましょう', 'ます', ('助動詞',))
        for text, raw, expected in [
            ('読みましょう', [verb, polite], ['ましょう']),
            ('読み ましょう', [verb, polite], []),
            ('本ましょう', [tok('本', '本', ('名詞',)), polite], []),
        ]:
            tokens, error = aligned_tokens(text, raw)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_mashou_you(tokens)], expected)
        source = sentence('今日は一緒に本を読もう。', [
            tok('今日は一緒に本を', '本', ('名詞',)),
            tok('読もう', '読む', ('動詞', '一般', '*', '*', '五段', '意志推量形')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        stage.process(Artifact([source]))
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[MASHOU_YOU_EXPRESSION].sentences[0].surface_form, '読もう')
        artifact = Artifact([source])
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[1].get('grammar_reserved', False))


if __name__ == '__main__':
    unittest.main()
