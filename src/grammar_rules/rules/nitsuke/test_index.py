import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitsuke.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NitsukeTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            'この歌を聞くにつけ故郷を思い出す。': ['につけ'],
            '雨につけ故郷を思う。': ['につけ'],
            '雨につけ風につけ故郷を思う。': [],
            '壁に付けた。': [],
            '壁につけて使う。': [],
            '雨に つけ思う。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(tokens)], expected)

    def test_reservation_and_identity(self):
        text = '雨につけ故郷を思う。'
        raw = [tok('雨', '雨', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
               tok('つけ', 'つける', ('動詞',)), tok('故郷', '故郷', ('名詞',)),
               tok('を', 'を', ('助詞',)), tok('思う', '思う', ('動詞',)),
               tok('。', '。', ('補助記号',))]
        stage = GrammarStep()
        artifact = stage.process(Artifact([sentence(text, raw)]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[0].get('grammar_reserved'))
        self.assertTrue(artifact.data[0].tokens[1].get('grammar_reserved'))
        self.assertTrue(artifact.data[0].tokens[2].get('grammar_reserved'))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'につけ')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
