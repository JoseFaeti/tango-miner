import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.rules.nikawari.index import RULE, match_nikawari
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NikawariTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'社長に代わり挨拶します。': 'に代わり',
                 '父にかわり出席する。': 'にかわり',
                 '父に代わり、私が出席した。': 'に代わり',
                 '父に代わります。': None, '色に変わります。': None,
                 '時代に代わりがない。': None, '父に代わって出席した。': None,
                 '父に 代わり出席した。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikawari(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('父に代わり私が出席した。', [
            tok('父', '父', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('代わり', '代わる', ('動詞',)), tok('私が出席した。', '私', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '代わり'])
        words = {'父': object()}
        stage.add_cards(words)
        self.assertIn('父', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に代わり')
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
