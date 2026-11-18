import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import TSUIDENI_EXPRESSION
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.tsuideni.index import match_tsuideni
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class TsuideniTests(unittest.TestCase):
    def test_sudachi_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '買い物のついでに寄る。': 'ついでに',
            '行くついでに買う。': 'ついでに',
            '行ったついでに買った。': 'ついでに',
            'ついでに買う。': 'ついでに',
            'ついでの用事だ。': None, '継いでに行く。': None,
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                aligned, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_tsuideni(aligned)],
                                 [] if expected is None else [expected])

    def test_card_reserves_pair_and_preserves_attached_word(self):
        source = sentence('買い物のついでに寄る。', [
            tok('買い物', '買い物', ('名詞',)), tok('の', 'の', ('助詞', '格助詞')),
            tok('ついで', 'ついで', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
            tok('寄る。', '寄る', ('動詞',))])
        stage = GrammarStep()
        data = stage.process(Artifact([source, source]))
        ExcludeGrammarTokensStep(stage).process(data)
        reserved = [t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')]
        self.assertIn('ついで', reserved)
        self.assertIn('に', reserved)
        self.assertNotIn('買い物', reserved)
        words = {'買い物': object()}
        stage.add_cards(words)
        self.assertIn('買い物', words)
        self.assertEqual(words[TSUIDENI_EXPRESSION].frequency, 2)
        self.assertEqual(words[TSUIDENI_EXPRESSION].sentences[0].surface_form, 'ついでに')
