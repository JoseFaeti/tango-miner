import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.rules.nai.index import RULE, match_nai
from src.grammar_rules.rules.obligation.index import match_obligation
from src.grammar_rules.rules.elliptical_obligation.index import match_elliptical_obligation
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NaiTests(unittest.TestCase):
    def test_observed_forms_and_lexical_uses(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'本を読まない。': 'ない', '本を読まなかった。': 'なかった',
                 '本を読まなかったら困る。': 'なかったら', '行かなきゃ困る。': 'なきゃ',
                 '行かなけりゃ困る。': 'なけりゃ',
                 '行かなくったって困る。': 'なくったって',
                 '行かなくったっていい。': None,
                 '本を読まなくていい。': 'なく', '本を読まないで。': 'ない',
                 '食べられない。': 'ない', '本を読まないです。': 'ない',
                 'お金がない。': None, '本が無い。': None,
                 '本を読みたくない。': None, '美しくない。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nai(aligned)],
                                 [] if expected is None else [expected])

    def test_conditional_and_incomplete_past(self):
        verb = tok('読ま', '読む', ('動詞', '*', '*', '*', '五段', '未然形-一般'))
        for text, tail, expected in [
            ('読まなければ', [tok('なけれ', 'ない', ('助動詞',)), tok('ば', 'ば', ('助詞',))], ['なければ']),
            ('読まなくった', [tok('なくっ', 'ない', ('助動詞',)), tok('た', 'た', ('助動詞',))], []),
            ('読まなくっ たって', [tok('なくっ', 'ない', ('助動詞',)), tok('たって', 'たって', ('助詞',))], []),
            ('読まなかっ', [tok('なかっ', 'ない', ('助動詞',))], [])]:
            aligned, error = aligned_tokens(text, [verb] + tail)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_nai(aligned)], expected)

    def test_obligation_cards_remain_available_with_conditional_negative(self):
        verb = tok('行か', '行く', ('動詞', '*', '*', '*', '五段', '未然形-一般'))
        negative = tok('なきゃ', 'ない', ('助動詞',))
        for text, tail, matcher, expected in [
            ('行かなきゃ。', [tok('。', '。', ('補助記号',))], match_elliptical_obligation, 'なきゃ'),
            ('行かなきゃいけない。', [tok('いけ', 'いける', ('動詞',)),
                                  tok('ない', 'ない', ('助動詞',)), tok('。', '。', ('補助記号',))],
             match_obligation, 'なきゃいけない')]:
            aligned, error = aligned_tokens(text, [verb, negative] + tail)
            self.assertIsNone(error)
            self.assertEqual([text[a:b] for a, b in match_nai(aligned)], ['なきゃ'])
            self.assertEqual([text[a:b] for a, b in matcher(aligned)], [expected])

    def test_card_reserves_only_negative_and_preserves_verb_and_lexical_nai(self):
        source = sentence('私は本を読まないで帰ります。', [
            tok('私は本を', '本', ('名詞',)),
            tok('読ま', '読む', ('動詞', '*', '*', '*', '五段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞',)), tok('で', 'で', ('助詞',)),
            tok('帰ります。', '帰る', ('動詞',))])
        lexical = sentence('お金がないので帰ります。', [tok('お金が', 'お金', ('名詞',)),
            tok('ない', 'ない', ('形容詞',)), tok('ので帰ります。', '帰る', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source, lexical]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['ない'])
        self.assertFalse(any(t.get('grammar_reserved') for t in data.data[1].tokens))
        words = {'読む': object(), 'ない': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ない')
        self.assertIn('読む', words)
        self.assertIn('ない', words)


if __name__ == '__main__':
    unittest.main()
