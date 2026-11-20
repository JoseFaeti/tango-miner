import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.GrammarConstants import GRAMMAR_EXPRESSIONS
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitsukenitsuke.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok


class NitsukenitsukeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '雨につけ風につけ故郷を思う。': ['につけ'],
            '嬉しいにつけ悲しいにつけ涙が出る。': ['につけ'],
            '良いにつけ悪いにつけ変わらない。': ['につけ'],
            '見るにつけ聞くにつけ心配だ。': ['につけ'],
            '雨につけ、風につけ故郷を思う。': ['につけ'],
            '雨につけ風につけ雪につけ思う。': ['につけ', 'につけ'],
            '雨につけ故郷を思う。': [],
            '壁に付け、机に付けた。': [],
            '雨につけ。風につけ思う。': [],
            '雨に つけ風につけ思う。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(tokens)], expected)

    def test_reservation_and_identity(self):
        text = '雨につけ風につけ思う。'
        raw = [tok('雨', '雨', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
               tok('つけ', 'つける', ('動詞',)), tok('風', '風', ('名詞',)),
               tok('に', 'に', ('助詞', '格助詞')), tok('つけ', 'つける', ('動詞',)),
               tok('思う', '思う', ('動詞',)), tok('。', '。', ('補助記号',))]
        stage = GrammarStep()
        artifact = stage.process(Artifact([sentence(text, raw)]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[3].get('grammar_reserved'))
        self.assertTrue(artifact.data[0].tokens[1].get('grammar_reserved'))
        self.assertTrue(artifact.data[0].tokens[2].get('grammar_reserved'))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        words = {'風': object()}
        stage.add_cards(words)
        self.assertIn('風', words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'につけ')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
