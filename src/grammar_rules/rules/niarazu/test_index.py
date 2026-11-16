import unittest

from src.Artifact import Artifact
from src.Grammar import GrammarStep, ExcludeGrammarTokensStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.niarazu.index import RULE, match_niarazu
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NiarazuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {'人にあらず。': 'にあらず', '人に非ず。': 'に非ず',
                 '人に有らず。': 'に有らず', '人にあらざる者。': 'にあらざる',
                 '人にあらざれば。': 'にあらざれば', 'ここにあらず。': 'にあらず',
                 '人にあらずして誰。': 'にあらず', '人にある。': None,
                 '洗わずに行く。': None, 'にあらず。': None, '人に あらず。': None}
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in match_niarazu(tokens)],
                                 [] if expected is None else [expected])

    def test_reservation_and_identity(self):
        source = sentence('彼は普通の人にあらず。', [
            tok('彼は普通の', '彼', ('名詞',)), tok('人', '人', ('名詞',)), tok('に', 'に', ('助詞',)),
            tok('あら', 'ある', ('動詞',)), tok('ず', 'ず', ('助動詞',)),
            tok('。', '。', ('補助記号',))])
        stage = GrammarStep()
        artifact = stage.process(Artifact([source]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertEqual([t['surface'] for t in artifact.data[0].tokens if t.get('grammar_reserved')],
                         ['に', 'あら', 'ず'])
        words = {'人': object()}
        stage.add_cards(words)
        self.assertIn('人', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'にあらず')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
