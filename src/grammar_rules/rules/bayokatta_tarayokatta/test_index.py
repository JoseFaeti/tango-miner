import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.bayokatta_tarayokatta.index import RULE, match_bayokatta_tarayokatta
from src.grammar_rules.rules.ba.index import RULE as BA
from src.grammar_rules.rules.tara.index import RULE as TARA
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class BayokattaTarayokattaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '早く行けばよかった。': 'ばよかった', '本を読んだら良かった。': 'だら良かった',
            '行ったらよかった。': 'たらよかった', '行かなければよかった。': 'ばよかった',
            '雨が降らなかったらよかった。': 'たらよかった',
            '天気がよければよかった。': 'ばよかった', '行けばいい。': None,
            '行ったら悪かった。': None, '行けば よかった。': None,
            'たらよかった。': None, '例えばよかった。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_bayokatta_tarayokatta(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_conditional_overlap(self):
        source = sentence('本を読めばよかった。', [
            tok('本を', '本', ('名詞',)),
            tok('読め', '読む', ('動詞', '一般', '*', '*', '五段', '仮定形-一般')),
            tok('ば', 'ば', ('助詞', '接続助詞')),
            tok('よかっ', '良い', ('形容詞', '一般', '*', '*', '形容詞', '連用形-促音便')),
            tok('た', 'た', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, BA, TARA))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ば', 'よかっ', 'た'])
        self.assertEqual(stage.occurrences[BA.id].count, 1)
        words = {'読む': object()}
        stage.add_cards(words)
        self.assertIn('読む', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ばよかった')


if __name__ == '__main__':
    unittest.main()
