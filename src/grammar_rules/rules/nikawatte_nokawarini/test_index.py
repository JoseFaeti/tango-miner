import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nikawatte_nokawarini.index import RULE, match_nikawatte_nokawarini
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NikawatteTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'父に代わって挨拶した。': 'に代わって',
                 '母にかわって出席する。': 'にかわって',
                 '牛乳の代わりに豆乳を使う。': 'の代わりに',
                 '私のかわりに行って。': 'のかわりに',
                 '父が代わってくれた。': None,
                 '父に代わる人を探す。': None,
                 '代わりに行く。': None,
                 '父の代わりを探す。': None,
                 '父に 代わって出席した。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nikawatte_nokawarini(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('父の代わりに私が出席しました。', [
            tok('父', '父', ('名詞',)), tok('の', 'の', ('助詞',)),
            tok('代わり', '代わり', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('私が出席しました。', '私', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['の', '代わり', 'に'])
        words = {'父': object()}
        stage.add_cards(words)
        self.assertIn('父', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'の代わりに')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
