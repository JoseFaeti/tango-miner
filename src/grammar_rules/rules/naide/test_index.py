
import unittest
from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.naide.index import RULE, match_naide
from src.grammar_rules.rules.nai.index import RULE as NAI
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NaideTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '食べないでください。': 'ないでください',
            '読まないで下さい。': 'ないで下さい',
            '行かないでくれ。': 'ないでくれ',
            '食べないで帰る。': 'ないで',
            '行かなくてもいいが、読まないで帰る。': 'ないで',
            '行かないで。': 'ないで',
            '食べられないで困る。': 'ないで',
            'お金がない。': None,
            '静かでないです。': None,
            '食べない。': None,
            '食べない でください。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naide(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_card_and_negative_overlap(self):
        source = sentence('これを食べないでください。', [
            tok('これを', 'これ', ('名詞',)),
            tok('食べ', '食べる', ('動詞', '一般', '*', '*', '下一段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞',)),
            tok('で', 'で', ('助詞', '接続助詞')),
            tok('ください', 'くださる', ('動詞', '非自立可能', '*', '*', '五段', '命令形')),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE, NAI))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['ない', 'で', 'ください'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'ないでください')
        self.assertEqual(words[NAI.expression].frequency, 1)


if __name__ == '__main__':
    unittest.main()
