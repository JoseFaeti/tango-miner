import unittest

from src.Artifact import Artifact
from src.Grammar import ExcludeGrammarTokensStep, GrammarStep
from src.grammar_rules.alignment import aligned_tokens
from src.grammar_rules.registry import RULES
from src.grammar_rules.rules.nitsukenitsuke.index import RULE as PAIRED
from src.grammar_rules.rules.nitsuketemo.index import RULE
from src.grammar_rules.rules.te_iru.test_index import sentence, tok
from src.GrammarConstants import GRAMMAR_EXPRESSIONS


class NitsuketemoTests(unittest.TestCase):
    def test_analyzer_forms_and_controls(self):
        try:
            from sudachipy import dictionary, tokenizer
            analyzer = dictionary.Dictionary(config_path='sudachi.config.json', dict='full').create()
        except (ImportError, OSError, RuntimeError) as exc:
            self.skipTest(f'Sudachi full dictionary unavailable: {exc}')
        cases = {
            '何事につけても彼を思い出す。': ['につけても'],
            'この写真を見るにつけても故郷を思う。': ['につけても'],
            '雨につけても風につけても心配だ。': ['につけても', 'につけても'],
            '雨につけ故郷を思う。': [],
            '壁に付けても落ちる。': [],
            '壁につけて使う。': [],
            '雨に つけても思う。': [],
        }
        for text, expected in cases.items():
            with self.subTest(text=text):
                raw = [tok(m.surface(), m.dictionary_form(), m.part_of_speech())
                       for m in analyzer.tokenize(text, tokenizer.Tokenizer.SplitMode.C)]
                tokens, error = aligned_tokens(text, raw)
                self.assertIsNone(error)
                self.assertEqual([text[a:b] for a, b in RULE.match(tokens)], expected)
                if 'につけても風につけても' in text:
                    self.assertEqual(list(PAIRED.match(tokens)), [])

    def test_reservation_and_identity(self):
        text = '雨につけても故郷を思い出す。'
        raw = [tok('雨', '雨', ('名詞',)), tok('に', 'に', ('助詞', '格助詞')),
               tok('つけ', 'つける', ('動詞',)), tok('て', 'て', ('助詞',)),
               tok('も', 'も', ('助詞',)), tok('故郷', '故郷', ('名詞',)),
               tok('を', 'を', ('助詞',)), tok('思い出す', '思い出す', ('動詞',)),
               tok('。', '。', ('補助記号',))]
        stage = GrammarStep()
        artifact = stage.process(Artifact([sentence(text, raw)]))
        ExcludeGrammarTokensStep(stage).process(artifact)
        self.assertFalse(artifact.data[0].tokens[0].get('grammar_reserved'))
        self.assertTrue(all(t.get('grammar_reserved') for t in artifact.data[0].tokens[1:5]))
        self.assertEqual(stage.occurrences[RULE.id].count, 1)
        words = {}
        stage.add_cards(words)
        self.assertEqual(words[RULE.expression].sentences[0].surface_form, 'につけても')
        self.assertIn(RULE, RULES)
        self.assertIn(RULE.expression, GRAMMAR_EXPRESSIONS)
