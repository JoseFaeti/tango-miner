import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nakutesumu.index import RULE, match_nakutesumu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NakutesumuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行かなくて済む。': 'なくて済む',
            '食べなくて済みました。': 'なくて済みました',
            '来られなくて済んだ。': 'なくて済んだ',
            '行かなくって済む。': 'なくって済む',
            '行かなくてすむ。': 'なくてすむ',
            '待ち時間が全くなくて済んだ。': 'なくて済んだ',
            '行かないで済む。': None, '行かずに済む。': None,
            '行かなくて住む。': None, '行かなくて帰る。': None,
            '待ち時間がなくて困る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nakutesumu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_lexical_evidence(self):
        source = sentence('明日は行かなくて済んだと思います。', [
            tok('明日は', '明日', ('名詞',)),
            tok('行か', '行く', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('なく', 'ない', ('助動詞', '*', '*', '*', '助動詞-ナイ', '連用形-一般')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('済ん', '済む', ('動詞',)), tok('だ', 'だ', ('助動詞',)),
            tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        result = ExcludeGrammarTokensStep(stage).process(
            stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in result.data[0].tokens
                          if t.get('grammar_reserved')], ['なく', 'て', '済ん', 'だ'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form,
                         'なくて済んだ')


if __name__ == '__main__':
    unittest.main()
