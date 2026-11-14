import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.naidesumu.index import NAIDESUMU_EXPRESSION, match_naidesumu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NaidesumuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '行かないで済む。': 'ないで済む', '行かないで済んだ。': 'ないで済んだ',
            '食べないで済みました。': 'ないで済みました', '行かないで済めばいい。': 'ないで済めば',
            '行かないで済まない。': 'ないで済まない', '食べられないで済む。': 'ないで済む',
            '行かなくて済む。': None, '行かないで住む。': None,
            '静かでないです。': None, '行かないで帰る。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_naidesumu(tokens)],
                                 [] if expected is None else [expected])

    def test_card_reservation_and_evidence(self):
        source = sentence('明日は行かないで済むと思います。', [
            tok('明日は', '明日', ('名詞',)),
            tok('行か', '行く', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞', '*', '*', '*', '助動詞-ナイ', '連体形-一般')),
            tok('で', 'で', ('助詞', '接続助詞')),
            tok('済む', '済む', ('動詞', '一般')), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens
                          if t.get('grammar_reserved')], ['ない', 'で', '済む'])
        words = {'行く': object()}
        stage.add_cards(words)
        self.assertIn('行く', words)
        self.assertEqual(words[NAIDESUMU_EXPRESSION].sentences[0].surface_form, 'ないで済む')


if __name__ == '__main__':
    unittest.main()
