import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.karasuruto_karasureba.index import RULE, match_karasuruto_karasureba
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KarasurutoKarasurebaTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '私からすると不思議だ。': 'からすると',
            '親からすれば心配だ。': 'からすれば',
            '値段からすると安い。': 'からすると',
            '食べてからすると決めた。': None,
            '名前から すると分かる。': None,
            '名前からした判断だ。': None,
            '彼からすべてを聞いた。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_karasuruto_karasureba(aligned)],
                                 [expected] if expected else [])

    def test_card_and_reservation_preserve_noun(self):
        source = sentence('親からすれば心配だ。', [
            tok('親', '親', ('名詞', '普通名詞')),
            tok('から', 'から', ('助詞', '格助詞')),
            tok('すれ', 'する', ('動詞', '非自立可能')),
            tok('ば', 'ば', ('助詞', '接続助詞')),
            tok('心配だ。', '心配', ('名詞',))])
        stage = GrammarStep(rules=(RULE,))
        artifact = stage.process(Artifact([source]))
        words = {'親': object()}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].frequency, 1)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'からすれば')
        self.assertIn('親', words)
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens
                          if t.get('grammar_reserved')], ['から', 'すれ', 'ば'])


if __name__ == '__main__':
    unittest.main()
