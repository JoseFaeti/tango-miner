import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_kudasai.index import RULE, match_te_kudasai
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TeKudasaiTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '食べてください。': 'てください', '読んで下さい。': 'で下さい',
            '食べてくれよ。': 'てくれ', '読んでくれ。': 'でくれ',
            '食べてくださいます。': None, '見てくださる。': None,
            '読んでくれる。': None, '食べてくれない。': None,
            '行ってくれた。': None, '食べないでください。': None,
            '本をください。': None, '雨でください。': None,
            '食べて ください。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_kudasai(aligned)],
                                 [] if expected is None else [expected])

    def test_reservation_and_card(self):
        verb = ('動詞', '一般', '*', '*', '下一段', '連用形-一般')
        request = ('動詞', '非自立可能', '*', '*', '五段', '命令形')
        source = sentence('これを食べてください。', [
            tok('これを', 'これ', ('名詞',)), tok('食べ', '食べる', verb),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('ください', 'くださる', request), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source, source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['て', 'ください'])
        words = {'食べる': object()}
        stage.add_cards(words)
        self.assertIn('食べる', words)
        self.assertEqual(words[RULE.expression].frequency, 2)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'てください')
        self.assertEqual(len(words[RULE.expression].sentences), 1)


if __name__ == '__main__':
    unittest.main()
