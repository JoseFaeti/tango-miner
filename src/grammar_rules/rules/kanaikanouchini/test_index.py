import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.kanaikanouchini.index import KANAIKANOUCHINI_EXPRESSION, match_kanaikanouchini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class KanaikanouchiniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '帰るか帰らないかのうちに電話が鳴った。': 'か帰らないかのうちに',
            '食べるか食べないかの内に出かけた。': 'か食べないかの内に',
            '来たか来ないかのうちに帰った。': 'か来ないかのうちに',
            'するかしないかのうちに終わった。': 'かしないかのうちに',
            '行くか帰らないかのうちに。': None,
            '帰るか帰らないかを決める。': None,
            '休みのうちに帰る。': None,
            '帰るか、帰らないかのうちに。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_kanaikanouchini(tokens)],
                                 [] if expected is None else [expected], raw)

    def test_card_and_attached_verb(self):
        source = sentence('帰るか帰らないかのうちに電話が鳴った。', [
            tok('帰る', '帰る', ('動詞',)), tok('か', 'か', ('助詞',)),
            tok('帰ら', '帰る', ('動詞', '一般', '*', '*', '五段', '未然形-一般')),
            tok('ない', 'ない', ('助動詞',)), tok('か', 'か', ('助詞',)),
            tok('の', 'の', ('助詞', '格助詞')), tok('うち', 'うち', ('名詞',)),
            tok('に', 'に', ('助詞', '格助詞')), tok('電話が鳴った。', '電話', ('名詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        self.assertFalse(data.data[0].tokens[0].get('grammar_reserved'))
        self.assertTrue(all(t.get('grammar_reserved') for t in data.data[0].tokens[1:8]))
        words = {'帰る': object()}
        stage.add_cards(words)
        self.assertIn('帰る', words)
        self.assertEqual(words[KANAIKANOUCHINI_EXPRESSION].frequency, 2)
        self.assertEqual(words[KANAIKANOUCHINI_EXPRESSION].sentences[0].surface_form, 'か帰らないかのうちに')
