import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.monodehanai.index import RULE, match_monodehanai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class MonodehanaiTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '人の悪口を言うものではない。': 'ものではない',
            '嘘をつくものじゃない。': 'ものじゃない',
            '人を傷つけるものではありません。': 'ものではありません',
            'そんなことを言うものではなかった。': 'ものではなかった',
            'そんなことをするものではないです。': 'ものではないです',
            'これは私のものではない。': None,
            '大切なものではない。': None,
            '人の悪口を言うものだ。': None,
            '食べるものではない。': 'ものではない',  # Also a literal thing.
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_monodehanai(aligned)],
                                 [] if expected is None else [expected])

    def test_card_and_boundaries(self):
        verb = tok('言う', '言う', ('動詞', '*', '*', '*', '*', '終止形-一般'))
        mono = tok('もの', 'もの', ('名詞', '普通名詞'))
        de = tok('で', 'で', ('助詞', '格助詞'))
        wa = tok('は', 'は', ('助詞', '係助詞'))
        nai = tok('ない', 'ない', ('形容詞',))
        source = sentence('人の悪口を言うものではない。',
                          [tok('人の悪口を', '人', ('名詞',)), verb, mono, de, wa, nai,
                           tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(any(t.get('grammar_reserved') for t in reserved.data[0].tokens))
        words = {'言う': object(), 'もの': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ものではない')
        self.assertIn('もの', words)
        for text, raw in [('言う ものではない', [verb, mono, de, wa, nai]),
                          ('言うものでは', [verb, mono, de, wa])]:
            aligned, error = aligned_tokens(text, raw)
            if error is None:
                self.assertEqual(list(match_monodehanai(aligned)), [])


if __name__ == '__main__':
    unittest.main()
