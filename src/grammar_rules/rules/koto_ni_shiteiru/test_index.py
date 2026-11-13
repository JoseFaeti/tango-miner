import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.koto_ni_shiteiru.index import RULE, match_koto_ni_shiteiru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KotoNiShiteiruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '毎日歩くことにしている。': 'ことにしている',
            '夜は食べないことにしています。': 'ことにしています',
            '毎日読むことにしていた。': 'ことにしていた',
            '毎日読むことにしてる。': 'ことにしてる',
            'そんなことにしている。': None,
            '明日行くことにした。': None,
            '行くことにしてみる。': None,
            '行くことに している。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_koto_ni_shiteiru(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card(self):
        source = sentence('毎日歩くことにしているよ。', [
            tok('毎日', '毎日', ('名詞',)),
            tok('歩く', '歩く', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('こと', 'こと', ('名詞', '普通名詞')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('し', 'する', ('動詞', '非自立可能', '*', '*', 'サ行変格', '連用形-一般')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('いる', 'いる', ('動詞', '非自立可能', '*', '*', '上一段', '終止形-一般')),
            tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['こと', 'に', 'し', 'て', 'いる'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ことにしている')


if __name__ == '__main__':
    unittest.main()
