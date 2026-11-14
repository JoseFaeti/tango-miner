import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tesumu.index import TESUMU_EXPRESSION, match_tesumu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TesumuTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '謝って済む。': 'て済む', '謝って済んだ。': 'て済んだ',
            '謝って済みました。': 'て済みました', '謝って済めばいい。': 'て済めば',
            '謝って済まない。': 'て済まない', '読んで済む。': 'で済む',
            '行かなくて済む。': None, 'けがで済んだ。': None,
            '食べて住む。': None, '払うだけで済む。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tesumu(tokens)],
                                 [] if expected is None else [expected])

    def test_card_keeps_lexical_evidence(self):
        source = sentence('謝って済むと思います。', [
            tok('謝っ', '謝る', ('動詞', '一般', '*', '*', '五段', '連用形-促音便')),
            tok('て', 'て', ('助詞', '接続助詞')),
            tok('済む', '済む', ('動詞', '一般')), tok('と思います。', 'と', ('助詞',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source, source]))
        reserved = ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in reserved.data[0].tokens
                          if t.get('grammar_reserved')], ['て', '済む'])
        words = {'謝る': object()}
        stage.add_cards(words)
        self.assertIn('謝る', words)
        self.assertEqual(words[TESUMU_EXPRESSION].frequency, 2)
        self.assertEqual(words[TESUMU_EXPRESSION].sentences[0].surface_form, 'て済む')


if __name__ == '__main__':
    unittest.main()
