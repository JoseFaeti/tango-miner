import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.obligation import RULE, match_obligation
from tests.test_grammar_te_iru import sentence, tok


class ObligationTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'行かなければならない。': 'なければならない',
                 '行かなきゃいけない。': 'なきゃいけない',
                 '行かなくちゃならない。': 'なくちゃならない',
                 '食べなければなりません。': 'なければなりません',
                 '行かないといけない。': 'ないといけない',
                 '行かなければ困る。': None, '行かない。': None,
                 '行かなければならなかった。': 'なければならなかった',
                 '行かなきゃいけなかった。': 'なきゃいけなかった',
                 '食べなくてはいけませんでした。': 'なくてはいけませんでした',
                 '行かなければなりませんでした。': 'なければなりませんでした',
                 '行かなければならなかったら困る。': None, '行か なきゃいけない。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_obligation(aligned)],
                                 [] if expected is None else [expected])

    def test_incomplete_past_does_not_reserve_present_prefix(self):
        verb = ('動詞', '一般', '*', '*', '五段', '未然形-一般')
        for suffix in (['なりません', 'でし'], ['なら', 'なかっ']):
            parts = [tok('行か', '行く', verb)] + [tok(x, x, ('助動詞', '*')) for x in ['なければ'] + suffix]
            text = ''.join(t['surface'] for t in parts)
            aligned, error = aligned_tokens(text, parts)
            self.assertIsNone(error)
            self.assertEqual(list(match_obligation(aligned)), [])

    def test_reservation_and_card(self):
        source = sentence('殺さなければならない。', [
            tok('殺さ', '殺す', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('なけれ', 'ない', ('助動詞', '*')), tok('ば', 'ば', ('助詞', '*')),
            tok('なら', 'なる', ('動詞', '*')), tok('ない', 'ない', ('助動詞', '*')),
            tok('。', '。', ('補助記号', '*'))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['なけれ', 'ば', 'なら', 'ない'])
        self.assertFalse(data.data[0].tokens[0].get('grammar_reserved'))
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'なければならない')


if __name__ == '__main__':
    unittest.main()
