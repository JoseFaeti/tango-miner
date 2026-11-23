import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nimuketa.index import RULE, match_nimuketa
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class NimuketaTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'試験に向けた準備。': 'に向けた', '子供に向けた本。': 'に向けた',
                 '彼にむけた手紙。': 'にむけた', '目標に向けた新しい取り組み。': 'に向けた',
                 '銃を窓に向けた。': None, '顔を向けた人。': None,
                 '試験に向けて準備する。': None, '試験に 向けた準備。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_nimuketa(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_identity_and_source(self):
        source = sentence('試験に向けた準備を始めます。', [
            tok('試験', '試験', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('向け', '向ける', ('動詞',)), tok('た', 'た', ('助動詞',)),
            tok('準備', '準備', ('名詞',)), tok('を始めます。', '始める', ('動詞',))])
        stage = GrammarStep()
        data = ExcludeGrammarTokensStep(stage).process(stage.process(Artifact([source])))
        self.assertEqual([t['surface'] for t in data.data[0].tokens if t.get('grammar_reserved')],
                         ['に', '向け', 'た'])
        words = {'試験': object(), '準備': object()}
        stage.add_cards(words)
        self.assertIn('試験', words)
        self.assertIn('準備', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'に向けた')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
