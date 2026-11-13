import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.koto_ni_natteiru.index import RULE, match_koto_ni_natteiru
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KotoNiNatteiruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '明日出発することになっている。': 'ことになっている',
            'ここでは泳がないことになっています。': 'ことになっています',
            '毎日掃除することになっていた。': 'ことになっていた',
            '明日行くことになってる。': 'ことになってる',
            'そんなことになっている。': None,
            '行くことになった。': None,
            '行くことになってみる。': None,
            '行くことに なっている。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_koto_ni_natteiru(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_card_and_identity(self):
        source = sentence('明日行くことになっているよ。', [
            tok('明日', '明日', ('名詞',)),
            tok('行く', '行く', ('動詞', '一般', '*', '*', '五段', '連体形-一般')),
            tok('こと', 'こと', ('名詞', '普通名詞')),
            tok('に', 'に', ('助詞', '格助詞')),
            tok('なっ', 'なる', ('動詞', '非自立可能', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('いる', 'いる', ('動詞', '非自立可能', '*', '*', '上一段', '終止形-一般')),
            tok('よ。', 'よ', ('助詞',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertFalse(result.data[0].tokens[1].get('grammar_reserved'))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['こと', 'に', 'なっ', 'て', 'いる'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ことになっている')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)


if __name__ == '__main__':
    unittest.main()
