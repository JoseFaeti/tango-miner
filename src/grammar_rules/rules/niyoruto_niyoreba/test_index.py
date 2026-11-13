import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niyoruto_niyoreba.index import RULE, match_niyoruto_niyoreba
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiyorutoNiyorebaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'天気予報によると雨だ。': 'によると', '彼によれば本当だ。': 'によれば',
                 'これによると明日だ。': 'によると', '駅に寄ると店がある。': None,
                 '駅に寄れば会える。': None, '事故による影響。': None,
                 '人によって違う。': None, '彼に よると本当だ。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niyoruto_niyoreba(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼によれば明日は雨だそうだ。', [
            tok('彼', '彼', ('代名詞',)), tok('に', 'に', ('助詞',)),
            tok('よれ', 'よる', ('動詞',)), tok('ば', 'ば', ('助詞',)),
            tok('明日', '明日', ('名詞',)), tok('は', 'は', ('助詞',)),
            tok('雨', '雨', ('名詞',)), tok('だそうだ。', 'だ', ('助動詞',))])
        stage = GrammarStep(rules=(RULE,))
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'よれ', 'ば'])
        words = {'彼': object(), '雨': object()}
        stage.add_cards(words)
        self.assertIn('彼', words)
        self.assertIn('雨', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'によれば')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
