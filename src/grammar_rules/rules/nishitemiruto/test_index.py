import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nishitemiruto.index import RULE, match_nishitemiruto
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NishitemirutoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'彼にしてみると当然だ。': 'にしてみると',
                 '子供にしてみると大問題だ。': 'にしてみると',
                 '色を赤にしてみるといい。': 'にしてみると',
                 '彼にして見ると分かる。': None, '彼にしてみれば困る。': None,
                 '彼にしてみたら困る。': None, '彼に してみると当然だ。': None,
                 '本をしてみるといい。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nishitemiruto(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼にしてみると当然だ。', [
            tok('彼', '彼', ('代名詞',)), tok('に', 'に', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('みる', 'みる', ('動詞',)), tok('と', 'と', ('助詞',)),
            tok('当然だ。', '当然', ('名詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'し', 'て', 'みる', 'と'])
        words = {'彼': object()}
        stage.add_cards(words)
        self.assertIn('彼', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にしてみると')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
