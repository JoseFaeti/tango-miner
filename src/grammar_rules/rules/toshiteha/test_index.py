import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.toshiteha.index import RULE, match_toshiteha
from src.grammar_rules.rules.toshite.index import RULE as TOSHITE_RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class ToshitehaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'代表としては賛成だ。': True, '私としては困る。': True,
                 '初めてとしては上手だ。': True, '教師として働く。': False,
                 '帰ろうとしては失敗する。': False, '仕事をしては休む。': False,
                 '代表として は賛成だ。': False}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_toshiteha(tokens)],
                                 ['としては'] if expected else [])

    def test_reservation_overlap_and_identity(self):
        source = sentence('彼は代表としては賛成しています。', [
            tok('彼', '彼', ('名詞',)), tok('は', 'は', ('助詞',)),
            tok('代表', '代表', ('名詞',)), tok('と', 'と', ('助詞',)),
            tok('し', 'する', ('動詞',)), tok('て', 'て', ('助詞',)),
            tok('は', 'は', ('助詞',)), tok('賛成しています。', '賛成', ('名詞',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['と', 'し', 'て', 'は'])
        words = {'代表': object()}
        stage.add_cards(words)
        self.assertIn('代表', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'としては')
        self.assertEqual(words[TOSHITE_RULE.expression].sentences[0].surface_form, 'として')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
