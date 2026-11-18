import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.katagata.index import KATAGATA_EXPRESSION, match_katagata
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KatagataTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'お礼かたがた伺います。': 'かたがた',
            '散歩かたがた買い物に行く。': 'かたがた',
            'ご挨拶方々伺います。': '方々',
            '見舞いかたがた訪ねる。': 'かたがた',
            'あの方々に会う。': None,
            '方々を歩く。': None,
            '先生方が集まる。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_katagata(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_boundaries(self):
        source = sentence('先生にお礼かたがた伺う。', [
            tok('先生に', '先生', ('名詞',)), tok('お礼', 'お礼', ('名詞',)), tok('かたがた', 'かたがた', ('名詞',)),
            tok('伺う', '伺う', ('動詞',)), tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['かたがた'])
        words = {'お礼': object()}
        stage.add_cards(words)
        self.assertIn('お礼', words)
        self.assertEqual(words[KATAGATA_EXPRESSION].frequency, 2)
        self.assertEqual(words[KATAGATA_EXPRESSION].sentences[0].surface_form, 'かたがた')
        tokens, error = aligned_tokens('お礼 かたがた伺う', [
            tok('お礼', 'お礼', ('名詞',)), tok('かたがた', 'かたがた', ('名詞',)),
            tok('伺う', '伺う', ('動詞',))])
        self.assertIsNone(error)
        self.assertEqual(list(match_katagata(tokens)), [])


if __name__ == '__main__':
    unittest.main()
