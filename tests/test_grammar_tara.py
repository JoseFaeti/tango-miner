import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarShadow import aligned_tokens
from src.grammar_rules.rules.tara import RULE, match_tara
from tests.test_grammar_te_iru import sentence, tok


class TaraTests(unittest.TestCase):
    def test_observed_forms_and_fragments(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'雨が降ったら帰る。': 'たら', '読んだら寝る。': 'だら',
                 '寒かったら帰る。': 'たら', '静かだったら行く。': 'たら',
                 '読まなかったら困る。': 'たら', 'たら、帰る。': None,
                 'たらこを食べる。': None, '行ったら。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tara(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_preserves_predicate_and_unrelated_lexical_token(self):
        source = sentence('読んだら、たらこを食べる。', [
            tok('読ん', '読む', ('動詞', '*', '*', '*', '五段', '連用形-撥音便')),
            tok('だら', 'だ', ('助動詞', '*', '*', '*', '助動詞-タ', '仮定形-一般')),
            tok('、', '、', ('補助記号',)), tok('たらこ', 'たらこ', ('名詞',)),
            tok('を食べる。', '食べる', ('動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')], ['だら'])
        words = {'読む': object(), 'たらこ': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'だら')
        self.assertIn('読む', words)
        self.assertIn('たらこ', words)


if __name__ == '__main__':
    unittest.main()
