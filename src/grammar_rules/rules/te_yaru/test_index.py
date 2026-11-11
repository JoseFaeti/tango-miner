import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.te_yaru.index import RULE, match_te_yaru


class TeYaruTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '本を読んでやる。': 'でやる', '本を読んでやった。': 'でやった',
            '本を読んでやります。': 'でやります', '本を読んでやらない。': 'でやらない',
            '本を読んでやって。': 'でやって', '本を読んでやれば。': 'でやれば',
            '本を読んでやろう。': 'でやろう', '食べてやる。': 'てやる',
            '仕事をやる。': None, '本を読んでいる。': None,
            '手でやる。': None, '食べて やる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_te_yaru(tokens)],
                                 [] if expected is None else [expected])

    def test_card_and_reservation(self):
        source = sentence('今日は読んでやった。', [
            tok('今日は', '今日', ('名詞',)),
            tok('読ん', '読む', ('動詞', '一般', '*', '*', '五段', '連用形-撥音便')),
            tok('で', 'で', ('助詞', '接続助詞')),
            tok('やっ', 'やる', ('動詞', '非自立可能', '*', '*', '五段', '連用形-促音便')),
            tok('た', 'た', ('助動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep(rules=(RULE,))
        result = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens if t.get('grammar_reserved')],
                         ['で', 'やっ', 'た'])
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'でやった')


if __name__ == '__main__':
    unittest.main()
