import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.grammar_rules.rules.toiwazutoiwazu.index import RULE
from src.GrammarConstants import GRAMMAR_EXPRESSIONS, LEGACY_GRAMMAR_EXPRESSIONS


class ToiwazutoiwazuTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '昼といわず夜といわず働く。': ['といわず'],
            '手と言わず、足と言わず傷だらけだ。': ['と言わず'],
            '昼と言わず夜といわず朝と言わず働く。': ['と言わず', 'といわず'],
            '昼といわず働く。': [],
            '昼といわず。夜といわず働く。': [],
            '昼と いわず夜といわず働く。': [],
            '何も言わず家と言わず帰った。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(tokens)], expected)

    def test_reservation_and_identity(self):
        text = '昼といわず夜といわず働く。'
        raw = [tok('昼', '昼', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
               tok('いわ', 'いう', ('動詞',)), tok('ず', 'ず', ('助動詞',)),
               tok('夜', '夜', ('名詞',)), tok('と', 'と', ('助詞', '格助詞')),
               tok('いわ', 'いう', ('動詞',)), tok('ず', 'ず', ('助動詞',)),
               tok('働く', '働く', ('動詞',)), tok('。', '。', ('補助記号',))]
        stage = GrammarStep()
        artifact = stage.process(Artifact([sentence(text, raw)]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[4].get('grammar_reserved'))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        words = {'夜': object()}
        stage.add_cards(words)
        self.assertIn('夜', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'といわず')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
        self.assertEqual(LEGACY_GRAMMAR_EXPRESSIONS[RULE.expression + ' (grammar)'], RULE.expression)
